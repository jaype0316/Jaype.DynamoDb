using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime.Internal.Util;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.Json;
using System.Xml.Linq;

namespace Jaype.DynamoDb
{
    public class DynamoDbStoreOptions
    {
        internal bool CamelCaseTableName { get; private set; }
        internal bool PluralizeTableName { get; private set; }
        internal bool LowerCaseClassName { get; private set; }  
        internal bool CamelCaseProperties { get; private set; }

        public DynamoDbStoreOptions UseCamelCaseTableName()
        {
            CamelCaseTableName = true;
            return this;
        }

        public DynamoDbStoreOptions UsePluralTableName()
        {
            PluralizeTableName = true;
            return this;
        }

        public DynamoDbStoreOptions UseCamelCaseProperties()
        {
            CamelCaseProperties = true;
            return this;
        }
    }
    public sealed class DynamoDbStore
    {
        readonly IAmazonDynamoDB _client;
        readonly DynamoDbStoreOptions _dynamoDbOptions;
        readonly JsonSerializerOptions _jsonSerializerOptions;
        private Dictionary<string, string> _clrToDynamoTypesMap = new Dictionary<string, string>()
        {
            { "bool", "BOOL"},
            { "int32", "N"},
            { "decimal", "N"},
            { "double", "N"},
            { "float", "N"},
            { "string", "S"},
            { "datetime", "S"}
        }; //todo: move this out

        public DynamoDbStore(IAmazonDynamoDB client, Action<DynamoDbStoreOptions> options)
        {
            _dynamoDbOptions = new DynamoDbStoreOptions();
            options(_dynamoDbOptions);

            _client = client;
            _jsonSerializerOptions = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            }; 
        }

        public DynamoDbStore(string accessKey, string secretKey, Amazon.RegionEndpoint region, Action<DynamoDbStoreOptions> options) 
            : this(new AmazonDynamoDBClient(accessKey, secretKey, region), options) { }

        public async Task<T> GetSingleOrDefault<T>(Expression<Func<T, object>> pkeyExpression, object pkeyValue, 
                                                            Expression<Func<T, object>> skeyExpression, object sortKeyValue) where T : class
        {
            var instance = Activator.CreateInstance<T>();
            var tableName = DetermineTableName(instance);

            var partitionKeyProperty = pkeyExpression.GetPropertyName(); 
            var sortKeyProperty = skeyExpression.GetPropertyName();
                
            var pKeyAttributeValue = TryResolveKeyAttributeValue(instance, partitionKeyProperty);
            var sKeyAttributeValue = TryResolveKeyAttributeValue(instance, sortKeyProperty);

            var pKeyPropertySanitized = SanitizePropertyKeyName(partitionKeyProperty);
            var sKeyPropertySanitized = SanitizePropertyKeyName(sortKeyProperty);

            var getItemRequest = new GetItemRequest
            {
                TableName = tableName,
                Key = new Dictionary<string, AttributeValue>()
                {
                     { pKeyPropertySanitized, pKeyAttributeValue },
                     { sKeyPropertySanitized, sKeyAttributeValue }
                }
            };

            var response = await _client.GetItemAsync(getItemRequest);
            var itemAsDocument = Document.FromAttributeMap(response.Item);
            var asJson = itemAsDocument?.ToJson();
            return JsonSerializer.Deserialize<T>(asJson, _jsonSerializerOptions);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="pkeyExpression">The partition key to use for the table</param>
        /// <param name="skeyExpression">The sort key to use for the table</param>
        /// <returns></returns>
        public async Task<Response> CreateTable<T>(Expression<Func<T, object>> pkeyExpression, Expression<Func<T, object>> skeyExpression) where T : class
        {
            var instance = Activator.CreateInstance<T>();
            var tableName = this.DetermineTableName<T>(instance);
            var partitionKey = pkeyExpression.GetPropertyName();
            var sortKey = skeyExpression.GetPropertyName();

            var partitionKeySanitized = SanitizePropertyKeyName(partitionKey);
            var sortKeySanitized = SanitizePropertyKeyName(sortKey);
            var request = new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    DetermineAttributeDefinition(instance, partitionKey),
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement(partitionKeySanitized, KeyType.HASH)
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5, 
                    WriteCapacityUnits = 5 
                }
            };

            if(!string.IsNullOrWhiteSpace(sortKey))
            {
                var sortKeyAttrDefinition = DetermineAttributeDefinition(instance, sortKey);
                request.AttributeDefinitions.Add(sortKeyAttrDefinition);
                request.KeySchema.Add(new KeySchemaElement(sortKeySanitized, KeyType.RANGE));
            }

            var response = await _client.CreateTableAsync(request);
            return new Response(response.HttpStatusCode);
        }

        public async Task<Response> Save<T>(T item) where T: class
        {
            var tableName = this.DetermineTableName(item);
            var saveItem = this.DetermineAttributeValues(item);

            var request = new PutItemRequest()
            {
                TableName = tableName,
                Item = saveItem
            };

            var response = await _client.PutItemAsync(request);
            return new Response(response.HttpStatusCode);
        }

        public async Task<IReadOnlyCollection<T>> Query<T>(Expression<Func<T, object>> pkeyExpression, object pkeyValue) where T : class
        {
            var instance = Activator.CreateInstance<T>();
            var tableName = DetermineTableName(instance);

            var partitionKeyProperty = pkeyExpression.GetPropertyName(); 
            var pKeyPropertySanitized = SanitizePropertyKeyName(partitionKeyProperty);

            var queryRequest = new QueryRequest
            {
                TableName = tableName,
                KeyConditionExpression = $"{pKeyPropertySanitized} = :{pKeyPropertySanitized}",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { $":{pKeyPropertySanitized}", new AttributeValue(pkeyValue as string) }
                }
            };

            var queryResponse = await _client.QueryAsync(queryRequest);
            if(!queryResponse.Items.Any())
                return Enumerable.Empty<T>() as IReadOnlyCollection<T>;

            var items = new List<T>();
            foreach(var item in queryResponse.Items)
            {
                var itemAsJson = Document.FromAttributeMap(item).ToJson();
                var clrType = JsonSerializer.Deserialize<T>(itemAsJson, _jsonSerializerOptions);
                items.Add(clrType);
            }

            return items.AsReadOnly();
        }

        private string DetermineTableName<T>(T item) where T : class
        {
            var dynamoTableAttribute = Attribute.GetCustomAttribute(typeof(T), typeof(DynamoDbTableAttribute)) as DynamoDbTableAttribute;
            var resolvedTableName = dynamoTableAttribute == null ? typeof(T).Name : dynamoTableAttribute.Name;
            if (_dynamoDbOptions.PluralizeTableName)
                resolvedTableName = resolvedTableName.ToPlural();

            return _dynamoDbOptions.CamelCaseTableName ? resolvedTableName.ToCamelCase() : resolvedTableName;
        }

        private Dictionary<string, AttributeValue> DetermineAttributeValues<T>(T item) where T : class
        {
            var map =new Dictionary<string, AttributeValue>();
            var properties = typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            foreach (var property in properties)
            {
                if (!map.ContainsKey(property.Name))
                {
                    var attributeValue = this.TryResolveKeyAttributeValue(item, property.Name);
                    
                    map.Add(SanitizePropertyKeyName(property.Name), attributeValue);    
                }
            }
            return map;
        }

        private AttributeValue TryResolveKeyAttributeValue<TType, TMember>(TType type, TMember member)
        {
            try
            {
                AttributeValue attr = null;
                var clrProperty = type.GetType().GetProperty(member.ToString());
                var clrType = clrProperty.PropertyType.Name.ToLower();
                if (_clrToDynamoTypesMap.TryGetValue(clrType, out var dynamoDbType))
                {
                    attr = new AttributeValue();
                    var value = clrProperty.GetValue(type);
                    if (dynamoDbType == "N" || dynamoDbType == "S")
                        value = value.ToString(); //per aws docs, number types are sent as strings to dynamoDb

                    attr.GetType().GetProperty(dynamoDbType).SetValue(attr, value);
                }

                return attr;
            }
            catch (Exception)
            {
                return null;
            }
           
        }

        private AttributeDefinition DetermineAttributeDefinition<TType, TMember>(TType type, TMember member)
        {
            Func<string, ScalarAttributeType> mapper = (str) =>
            {
                return str switch
                {
                    "S" => ScalarAttributeType.S,
                    "N" => ScalarAttributeType.N,
                    "B" => ScalarAttributeType.B,
                    _ => throw new NotImplementedException()
                };
            };

            AttributeDefinition attr = null;
            var clrProperty = type.GetType().GetProperty(member.ToString());
            var clrType = clrProperty.PropertyType.Name.ToLower();
            if(_clrToDynamoTypesMap.TryGetValue(clrType, out var dynamoDbType))
            {
                var scalarAttributeType = mapper(dynamoDbType);
                attr = new AttributeDefinition(SanitizePropertyKeyName(member.ToString()), scalarAttributeType);
            }

            return attr;
        }


        private string SanitizePropertyKeyName(string keyName) => _dynamoDbOptions.CamelCaseProperties ? keyName.ToCamelCase() : keyName;
        
    }

   
}

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
        internal bool LowerCasePropertyName { get; private set; }

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
            LowerCasePropertyName = true;
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
                
            var pKeyAttributeValue = TryResolveKeyAttributeValue(partitionKeyProperty, pkeyValue);
            var sKeyAttributeValue = TryResolveKeyAttributeValue(sortKeyProperty, sortKeyValue);

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
        public async Task<CreateTableResponse> CreateTable<T>(Expression<Func<T, object>> pkeyExpression, Expression<Func<T, object>> skeyExpression) where T : class
        {
            var instance = Activator.CreateInstance<T>();
            var tableName = this.DetermineTableName<T>(instance);
            var partitionKey = pkeyExpression.GetPropertyName();
            var sortKey = skeyExpression.GetPropertyName();
          
            var request = new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    DetermineAttributeDefinition(partitionKey)
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement(partitionKey, KeyType.HASH)
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5, // Adjust according to your needs
                    WriteCapacityUnits = 5 // Adjust according to your needs
                }
            };

            if(!string.IsNullOrWhiteSpace(sortKey))
            {
                var sortKeyAttrDefinition = DetermineAttributeDefinition(sortKey);
                request.AttributeDefinitions.Add(sortKeyAttrDefinition);
                request.KeySchema.Add(new KeySchemaElement(sortKey, KeyType.RANGE));
            }

            var response = await _client.CreateTableAsync(request);
            return new CreateTableResponse(response.HttpStatusCode);
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

            return _dynamoDbOptions.CamelCaseTableName ? resolvedTableName.ToCamelCase() : resolvedTableName;
        }

        private void ThrowIfNull<T>(T item) where T: class
        {
            if(item is null) throw new ArgumentNullException(nameof(item));
        }

        private AttributeValue TryResolveKeyAttributeValue<TMember>(TMember member, object value)
        {
            try
            {
                AttributeValue attr = null;
                var pkeyPropertyClrType = member.GetType().Name.ToLower();
                if (_clrToDynamoTypesMap.TryGetValue(pkeyPropertyClrType, out var dynamoDbType))
                {
                    attr = new AttributeValue();
                    attr.GetType().GetProperty(dynamoDbType).SetValue(attr, value);
                }

                return attr;
            }
            catch (Exception)
            {
                return null;
            }
           
        }

        private AttributeDefinition DetermineAttributeDefinition<TMember>(TMember member)
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
            var clrType = member.GetType().Name.ToLower();
            if(_clrToDynamoTypesMap.TryGetValue(clrType, out var dynamoDbType))
            {
                var scalarAttributeType = mapper(dynamoDbType);
                attr = new AttributeDefinition(nameof(member), scalarAttributeType);
            }

            return attr;
        }


        private string SanitizePropertyKeyName(string keyName) => _dynamoDbOptions.LowerCasePropertyName ? keyName.ToLower() : keyName;
        
    }

   
}

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime.Internal.Util;
using Microsoft.VisualBasic;
using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
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
        const int BATCH_SAVE_THROTTLE_SECONDS = 2;
        const int BATCH_SAVE_ITEMS_COUNT_LIMIT = 25; //per dynamodb api restriction

        private Dictionary<string, string> _clrToDynamoTypesMap = new Dictionary<string, string>()
        {
            { "bool", "BOOL"},
            { "int32", "N"},
            { "decimal", "N"},
            { "double", "N"},
            { "float", "N"},
            { "string", "S"},
            { "datetime", "S"},
            { "ienumerable`1", "SS"},
            { "nullable`1", "N"},
            { "ilist`1", "L"}
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
            if (!await TableExists(tableName))
                throw new InvalidOperationException($"{tableName} does not exist");

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
            if (await TableExists(tableName))
                return new Response(System.Net.HttpStatusCode.BadRequest, message:"Table already exists");
            
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

        /// <summary>
        /// Creates an index against the sort key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="indexKeyExpression"></param>
        /// <param name="sortKeyExpression"></param>
        /// <param name="indexName"></param>
        /// <returns></returns>
        public async Task<Response> CreateIndex<T>(Expression<Func<T, object>> indexKeyExpression, Expression<Func<T, object>> indexSortExpression = null, string indexName = null) where T : class
        {
            var instance = Activator.CreateInstance<T>();
            var tableName = this.DetermineTableName<T>(instance);
            if (!await TableExists(tableName))
                return new Response(System.Net.HttpStatusCode.BadRequest, message: "Cannot create the index because the table doesn't exist. Create the table first");

            var indexKey = indexKeyExpression.GetPropertyName();
            var indexKeySanitized = this.SanitizePropertyKeyName(indexKey);
            indexName = indexName ?? this.InferIndexName(tableName, indexKeySanitized);

            var indexExists = await this.IndexExists(tableName, indexName);
            if (indexExists)
                return new Response(System.Net.HttpStatusCode.BadRequest, message: $"Index {indexName} already exists");

            var sortKey = indexSortExpression?.GetPropertyName();
            var sortKeySanitized = sortKey != null ? this.SanitizePropertyKeyName(sortKey) : null;
            var keySchema = new List<KeySchemaElement>
            {
                new KeySchemaElement(indexKeySanitized, KeyType.HASH)
            };
            if(!string.IsNullOrEmpty(sortKeySanitized))
                keySchema.Add(new KeySchemaElement(sortKeySanitized, KeyType.RANGE));

            var provisionedThroughput = new ProvisionedThroughput
            {
                ReadCapacityUnits = 5, 
                WriteCapacityUnits = 5 
            };

            // Create the update table request with the global secondary index
            var updateTableRequest = new UpdateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                   this.DetermineAttributeDefinition(instance, indexKey)
                },
                GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                {
                    new GlobalSecondaryIndexUpdate
                    {
                        Create = new CreateGlobalSecondaryIndexAction()
                        {
                             IndexName = indexName,
                             KeySchema = keySchema,
                             Projection = new Projection(){ ProjectionType = ProjectionType.ALL },
                             ProvisionedThroughput = provisionedThroughput
                        }
                    }
                }
            };
            if (!string.IsNullOrEmpty(sortKeySanitized))
                updateTableRequest.AttributeDefinitions.Add(this.DetermineAttributeDefinition(instance, sortKey));

            var response = await _client.UpdateTableAsync(updateTableRequest);
            return new Response(response.HttpStatusCode);
        }

        public async Task<Response> Save<T>(T item) where T: class
        {
            var tableName = this.DetermineTableName(item);
            if (!await TableExists(tableName))
                throw new InvalidOperationException($"{tableName} does not exist");

            var saveItem = this.DetermineAttributeValues(item);

            var request = new PutItemRequest()
            {
                TableName = tableName,
                Item = saveItem
            };

            var response = await _client.PutItemAsync(request);
            return new Response(response.HttpStatusCode);
        }
        public async Task<Response> BatchSave<T>(IList<T> items) where T : class
        {
            if (items.Count() == 0)
                return new Response(System.Net.HttpStatusCode.OK);

            var tableName = this.DetermineTableName(items.FirstOrDefault());
            if (!await TableExists(tableName))
                throw new InvalidOperationException($"{tableName} does not exist");
       
            var itemsToPut = new List<Dictionary<string, AttributeValue>>();
            var pagedItems = items.Skip(0).Take(BATCH_SAVE_ITEMS_COUNT_LIMIT).ToList();
                
            foreach (var item in pagedItems)
            {
                var itemToPut = this.DetermineAttributeValues(item);
                itemsToPut.Add(itemToPut);
            }

            var batchWriteRequest = new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    {
                        tableName,
                        itemsToPut.Select(item => new WriteRequest
                        {
                            PutRequest = new PutRequest
                            {
                                Item = new Dictionary<string, AttributeValue>(item)
                            }
                        }).ToList()
                    }
                }
            };

            await _client.BatchWriteItemAsync(batchWriteRequest);

            int pagedCounter = Math.Min(pagedItems.Count, BATCH_SAVE_ITEMS_COUNT_LIMIT);
            items = items.Skip(pagedCounter).ToList();

            //throttle
            Thread.Sleep(BATCH_SAVE_THROTTLE_SECONDS * 1000);
            return await this.BatchSave(items);                    
        }

        public async Task<IReadOnlyCollection<T>> Query<T>(Expression<Func<T, object>> pkeyExpression, object pkeyValue) where T : class
        {
            var instance = Activator.CreateInstance<T>();
            var tableName = DetermineTableName(instance);
            if (!await TableExists(tableName))
                throw new InvalidOperationException($"{tableName} does not exist");

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

        public async Task<IReadOnlyCollection<T>> QueryByIndex<T>(Expression<Func<T, object>> indexKeyExpression, string value, string indexName = null) where T : class
        {
            var instance = Activator.CreateInstance<T>();
            var tableName = DetermineTableName(instance);

            if (!await TableExists(tableName))
                throw new InvalidOperationException($"{tableName} does not exist");

            var indexKey = indexKeyExpression.GetPropertyName();
            var indexKeySanitized = SanitizePropertyKeyName(indexKey);

            indexName = indexName ?? this.InferIndexName(tableName, indexKeySanitized);
            // Create a Query operation with the sort key value and index name
            var filter = new QueryFilter(indexKeySanitized, QueryOperator.Equal, value);
            var query = new QueryRequest()
            {
                TableName = tableName,
                IndexName = indexName,
                KeyConditionExpression = $"#{indexKeySanitized} = :{indexKeySanitized}",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    { $"#{indexKeySanitized}", indexKeySanitized } // Replace "SortKey" with your actual sort key attribute name
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { $":{indexKeySanitized}", new AttributeValue(value) }
                }
            };

            var response = await _client.QueryAsync(query);
            var items = new List<T>();
            foreach (var item in response.Items)
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
            if (dynamoTableAttribute is null && _dynamoDbOptions.PluralizeTableName)
                resolvedTableName = resolvedTableName.ToPlural();

            return _dynamoDbOptions.CamelCaseTableName ? resolvedTableName.ToCamelCase() : resolvedTableName;
        }

        private async Task<bool> TableExists(string tableName)
        {
            var response = await _client.ListTablesAsync();
            return response.TableNames.Contains(tableName);
        }

        private async Task<bool> IndexExists(string tableName, string indexName)
        {
            var describeTableResponse = await _client.DescribeTableAsync(new DescribeTableRequest
            {
                TableName = tableName
            });

            // Check if the index exists in the table description
            return describeTableResponse.Table.GlobalSecondaryIndexes.Exists(i => i.IndexName == indexName);
        }

        private Dictionary<string, AttributeValue> DetermineAttributeValues<T>(T item) where T : class
        {
            var map =new Dictionary<string, AttributeValue>();
            var properties = item.GetType().GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
            foreach (var property in properties)
            {
                if (!map.ContainsKey(property.Name))
                {
                    var attributeValue = this.TryResolveKeyAttributeValue(item, property.Name);
                    if(attributeValue != null) 
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
                    {
                        value = value?.ToString(); //per aws docs, number types are sent as strings to dynamoDb
                        if (value == default)
                            return null;
                    }
                    if (dynamoDbType == "SS")
                    {
                        value = value as List<string> ?? new List<string>();
                        if ((value as List<string>).Count == default)
                            return null;
                    }
                    if (clrType == "nullable`1")
                    {
                        var underyingType = Nullable.GetUnderlyingType(clrProperty.PropertyType);
                        value = value?.ToString();                       
                    } 
                    else if(dynamoDbType == "L")
                    {
                        //var innerType = clrProperty.PropertyType.GetTypeInfo().GetGenericArguments()[0];
                        //IList unboxedList = ReflectionHelper.CastToList(innerType, value);
                        //var listContent = new List<AttributeValue>();

                        //foreach(var item in unboxedList)
                        //{
                        //    var listContentItem = DetermineAttributeValues(item);
                        //    listContent.Add(new AttributeValue() { M = listContentItem });
                        //} 
                        //value = listContent;
                        //override the L dynamo type, setting it to S and just serializing the value to a json string.
                        dynamoDbType = "S";
                        value = JsonSerializer.Serialize(value);
                    }
                      
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
            string clrMemberType = null;
            if(type.GetType().GetProperty(member.ToString()) != null)
            {
                var clrProperty = type.GetType().GetProperty(member.ToString());
                clrMemberType = clrProperty.PropertyType.Name.ToLower();
            } 
            else
            {
                clrMemberType = member.GetType().Name.ToLower();
            }

            if (_clrToDynamoTypesMap.TryGetValue(clrMemberType, out var dynamoDbType))
            {
                var scalarAttributeType = mapper(dynamoDbType);
                attr = new AttributeDefinition(SanitizePropertyKeyName(member.ToString()), scalarAttributeType);
            }

            return attr;
        }


        private string SanitizePropertyKeyName(string keyName) => _dynamoDbOptions.CamelCaseProperties ? keyName.ToCamelCase() : keyName;
        private string InferIndexName(string tableName, string partitionKey) => $"idx_{tableName}_{partitionKey}";
        
    }

   
}

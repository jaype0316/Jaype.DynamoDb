using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jaype.DynamoDb
{
    public sealed class DynamoDbPartitionKeyAttribute : Attribute
    {
    }

    public sealed class DynamoDbTableAttribute : Attribute
    {
        public string Name { get; private set; }
        public DynamoDbTableAttribute(string name) 
        {
            Name = name;
        }
    }
}

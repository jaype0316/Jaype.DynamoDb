using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Jaype.DynamoDb
{
    internal class ReflectionHelper
    {
        public static IList CastToList(Type type, object boxedList)
        {
            var listType = typeof(List<>).MakeGenericType(type);
            if (boxedList.GetType() == listType)
            {
                return (IList)boxedList;
            }

            throw new InvalidCastException("The boxed object is not of the expected type List<T>.");
        }
    }
}

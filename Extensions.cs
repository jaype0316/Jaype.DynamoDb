using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Jaype.DynamoDb
{
    internal static class Extensions
    {
        public static string GetPropertyName<T>(this Expression<Func<T, object>> exp)
        {
            var name = ""; 
            if (exp.Body is MemberExpression memberExpression)
            {
                name = memberExpression.Member.Name;
            }
            else if (exp.Body is UnaryExpression unaryExpression && unaryExpression.Operand is MemberExpression unaryMemberExpression)
            {
                name = unaryMemberExpression.Member.Name;
            }
            return name;
        }

        public static string ToCamelCase(this string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            string[] words = value.Split('_');
            if (words.Length == 1)
                return value.ToLower();

            string camelCasedString = words[0];

            for (int i = 1; i < words.Length; i++)
            {
                camelCasedString += char.ToUpper(words[i][0]) + words[i].Substring(1);
            }

            return camelCasedString;
        }

        public static string ToPlural(this string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            if (value.EndsWith("y", StringComparison.OrdinalIgnoreCase))
                return value.Substring(0, value.Length - 1) + "ies";

            return value + "s";
        }
    }
}

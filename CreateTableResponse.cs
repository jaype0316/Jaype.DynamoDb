using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Jaype.DynamoDb
{
    public record CreateTableResponse
    {
        public HttpStatusCode StatusCode { get; } = HttpStatusCode.OK;
        public CreateTableResponse(HttpStatusCode statusCode)
        {
            StatusCode = statusCode;
        }

    }
}

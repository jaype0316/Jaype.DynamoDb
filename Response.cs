using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Jaype.DynamoDb
{
    public record Response
    {
        public HttpStatusCode StatusCode { get; } = HttpStatusCode.OK;
        public string Message { get; }
        public Response(HttpStatusCode statusCode, string message = null)
        {
            StatusCode = statusCode;
            Message = message;
        }

    }
}

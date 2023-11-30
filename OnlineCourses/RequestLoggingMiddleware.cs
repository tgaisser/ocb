using System;
using System.Net;
using System.Net.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace Hillsdale.OnlineCourses
{

	//from https://blog.elmah.io/asp-net-core-request-logging-middleware/
	//and https://www.stevejgordon.co.uk/httpclientfactory-asp-net-core-logging
	public class RequestLoggingMiddleware
	{
		private readonly RequestDelegate _next;
		private readonly ILogger _logger;
		private static readonly EventId PipelineStart = new EventId(100, "RequestPipelineStart");
		private static readonly EventId PipelineEnd = new EventId(101, "RequestPipelineEnd");

		private static readonly Action<ILogger, string, string, string, Exception> RequestPipelineStart =
			LoggerMessage.Define<string, string, string>(
				LogLevel.Information,
				PipelineStart,
				"Start Request [{RequestID}] {method} {url}");

		private static readonly Action<ILogger, string, string, string, int?, Exception> RequestPipelineEnd =
			LoggerMessage.Define<string, string, string, int?>(
				LogLevel.Information,
				PipelineEnd,
				"End Request [{RequestID}] {method} {url} => {statusCode}");


		public RequestLoggingMiddleware(RequestDelegate next, ILoggerFactory loggerFactory)
		{
			_next = next;
			_logger = loggerFactory.CreateLogger<RequestLoggingMiddleware>();
		}

		public async Task Invoke(HttpContext context)
		{
			try
			{
				RequestPipelineStart(_logger, context.TraceIdentifier, context.Request?.Method, context.Request?.Path.Value, null);

				await _next(context);
			}
			finally
			{
				RequestPipelineEnd(_logger, context.TraceIdentifier, context.Request?.Method, context.Request?.Path.Value, context.Response?.StatusCode, null);

			}
		}
	}
}
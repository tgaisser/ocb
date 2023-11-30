using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Hillsdale.OnlineCourses.Controllers
{
	[Route("sso")]
	[Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
	[ApiController]
	public class SsoController : Controller
	{
		/// <summary>
		/// Provides a Logger scoped to this controller.
		/// </summary>
		protected readonly ILogger _logger;
		private CourseData _courseData;

		public SsoController(IOptions<ConnectionStringConfig> configAccessor, ILogger<CourseController> logger)
		{
			_logger = logger;
			_logger.LogInformation("Started SsoController");
			_courseData = new CourseData(configAccessor.Value, logger);
		}

		//https://meta.discourse.org/t/official-single-sign-on-for-discourse-sso/13045
		[HttpPost("validate")]
		public async Task<ActionResult> ValidateSSO(SsoRequest request)
		{
			_logger.LogDebug("Entering ValidateSSO {}, {}", request.Payload, request.Signature);
			/*
			 * Setup
			 */
			//pull out the relevant claims types from the token
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
			var email = HttpContext.User.FindFirst(ClaimTypes.Email)?.Value;
			var firstname = HttpContext.User.FindFirst(ClaimTypes.GivenName)?.Value;
			var lastname = HttpContext.User.FindFirst(ClaimTypes.Surname)?.Value;

			//If the user doesn't have a `sub` (e.g. id) or `email`, then bail.
			if (userId == null) return StatusCode(401, "Must be logged in.");
			if (email == null) return StatusCode(422, "User must have an email address");

			var userCourses = await _courseData.GetUserCourses(userId);
			var studyGroupIds = userCourses
				.Select(c => c.StudyGroupId)
				.Where(c => !string.IsNullOrWhiteSpace(c));

			//debug the information
			_logger.LogDebug("ValidateSSO: userId {}", userId);
			_logger.LogDebug("ValidateSSO: name {}", HttpContext.User.FindFirst(ClaimTypes.GivenName)?.Value);
			_logger.LogDebug("ValidateSSO: email {}", HttpContext.User.FindFirst(ClaimTypes.Email)?.Value);
			_logger.LogDebug("ValidateSSO: surname {}", HttpContext.User.FindFirst(ClaimTypes.Surname)?.Value);


			/*
			 * Validate incoming request
			 */
			//base64 decode the payload
			var decodedPayload = request.Payload.DecodeBase64();
			_logger.LogDebug("ValidateSSO: base64decode {}", decodedPayload);

			//parse the decoded value as a query string (should be "nonce=SOMETHING") and extract the nonce
			var parsedPayload = QueryHelpers.ParseQuery(decodedPayload);
			_logger.LogDebug("ValidateSSO: dict {}", parsedPayload);
			var nonce = parsedPayload["nonce"];
			_logger.LogDebug("ValidateSSO: nonce {}", nonce);

			//Validate that the payload was appropriately signed with our secret key
			if (GetHash(request.Payload, Startup.DiscourseSsoKey) != request.Signature)
			{
				return StatusCode(422, "Invalid hash");
			}

			/*
			 * Set up return request/URL
			 */
			//set up the parameters we're going to send back to Discourse
			var queryParams = new Dictionary<string, string>()
			{
				{"nonce", nonce}, //return the original nonce
				{
					"name", (!string.IsNullOrEmpty(firstname) ? $"{firstname} {lastname?.Trim()}".Trim() : email)
				}, //if we don't have names, use email
				{"username", email}, //use email
				{"email", email},
				{"external_id", userId},
				{"add_groups", string.Join(",", studyGroupIds)}
			};
			//create a querystring of the parameters
			var resultQs =
				QueryHelpers.AddQueryString(string.Empty, queryParams)
					.Substring(1); //use no root and remove trailing '?'
			_logger.LogDebug("ValidateSSO: new value {}", resultQs);

			//base 64 encode the query string
			var encodedQs = resultQs.EncodeBase64();
			_logger.LogDebug("ValidateSSO: encoded value {}", encodedQs);

			//create a signature of base64 encoded string
			var resultSig = GetHash(encodedQs, Startup.DiscourseSsoKey);
			_logger.LogDebug("ValidateSSO: result signature {}", resultSig);

			//Return the new URL with the encoded payload and signature
			var resultParams = new Dictionary<string, string>()
			{
				{"sso", encodedQs},
				{"sig", resultSig}
			};
			return Ok(QueryHelpers.AddQueryString(Startup.DiscourseSsoUrl, resultParams));
		}

		//https://stackoverflow.com/a/47686794
		public static string GetHash(string text, string key)
		{
			// change according to your needs, an UTF8Encoding
			// could be more suitable in certain situations
			var encoding = new ASCIIEncoding(); //TODO UTF8?

			var textBytes = encoding.GetBytes(text);
			var keyBytes = encoding.GetBytes(key);

			byte[] hashBytes;

			using (var hash = new HMACSHA256(keyBytes))
			{
				hashBytes = hash.ComputeHash(textBytes);

				return BitConverter.ToString(hashBytes).Replace("-", "").ToLower();
			}
		}
	}

	public class SsoRequest
	{
		public string Payload { get; set; }

		//public string quiz_id { get; set; }
		public string Signature { get; set; }
	}

	//	using System;
	//    using System.Text;
	//https://stackoverflow.com/a/56353378
	public static class Base64Conversions
	{
		public static string EncodeBase64(this string text, Encoding encoding = null)
		{
			if (text == null) return null;

			encoding = encoding ?? Encoding.UTF8;
			var bytes = encoding.GetBytes(text);
			return Convert.ToBase64String(bytes);
		}

		public static string DecodeBase64(this string encodedText, Encoding encoding = null)
		{
			if (encodedText == null) return null;

			encoding = encoding ?? Encoding.UTF8;
			var bytes = Convert.FromBase64String(encodedText);
			return encoding.GetString(bytes);
		}
	}
}
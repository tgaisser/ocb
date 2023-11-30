using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using Amazon.Lambda;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Hillsdale.OnlineCourses.Controllers
{
	[Route("/users")]
	[Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
	[ApiController]
	public class UserController : Controller
	{
		/// <summary>
		/// Provides all available connection strings
		/// </summary>
		private readonly ConnectionStringConfig _connectionStringConfig;

		/// <summary>
		/// Provides a Logger scoped to this controller.
		/// </summary>
		private readonly ILogger _logger;
		private IAmazonLambda _lambda;

		private readonly UserData _userData;

		public UserController(IOptions<ConnectionStringConfig> configAccessor, ILogger<CourseController> logger, IAmazonLambda lambda)
		{
			_connectionStringConfig = configAccessor.Value;
			_logger = logger;
			_logger.LogInformation("Started UserController");
			_lambda = lambda;
			_userData = new UserData(_connectionStringConfig, logger);
		}

		[HttpGet("me/preferences")]
		public async Task<ActionResult> GetPreferences()
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			return Ok(await _userData.GetUserSettings(userId));
		}
		[HttpPut("me/preferences")]
		public async Task<ActionResult> SavePreferences(UserSettings request)
		{
			request.UserId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
			request.LastUpdate = new DateTime();
			await _userData.SetUserSettings(request);
			return Ok(request);
		}
		[HttpPut("me/preferences/subject")]
		public async Task<ActionResult> SavePreferences([FromBody] string preference)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			return Ok(await _userData.UpdateUserSettingsWithSubject(userId, preference));
		}
		[HttpPut("me/preferences/preferAudio")]
		public async Task<ActionResult> SavePreferences([FromBody] bool preference)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			return Ok(await _userData.UpdateUserSettingsWithPreferAudio(userId, preference));
		}

		[HttpPut("me/merge-accounts")]
		public async Task<bool> MergeAccounts([FromBody] IEnumerable<UserMatch> accounts, [FromHeader(Name = "Authorization")] string authToken)
		{
			_logger.LogDebug("Called MergeUsers with {}", string.Join( ", ", accounts.Select(a => a.UserId)));
			return await _userData.MergeAccounts(authToken, accounts, _lambda);
		}

		[HttpPut("me/ignore-accounts")]
		public async Task<bool> IgnoreAccounts([FromBody] IEnumerable<UserMatch> accounts, [FromHeader(Name = "Authorization")] string authToken)
		{
			_logger.LogDebug("Called MergeUsers with {}", string.Join( ", ", accounts.Select(a => a.UserId)));
			return await _userData.IgnoreAccounts(authToken, accounts, _lambda);
		}

		[HttpPut("me/social-signin")]
		public async Task<bool> SaveSocialSignin([FromBody] UtmInfo analytics)
		{
			_logger.LogDebug("SaveSocialSignin:: {}", analytics.Stringify());
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
			var email = HttpContext.User.FindFirst(ClaimTypes.Email)?.Value;
			var username = HttpContext.User.FindFirst("cognito:username")?.Value;
			await _userData.SetUserUtmCodes(userId, email, username, analytics.Stringify());
			return true;
		}
	}

}

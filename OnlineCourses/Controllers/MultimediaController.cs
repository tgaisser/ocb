using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using VimeoDotNet;
using VimeoDotNet.Enums;
using VimeoDotNet.Models;

namespace Hillsdale.OnlineCourses.Controllers
{
	[Route("multimedia")]
	[Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
	[ApiController]
	public class MultimediaController : Controller
	{
		/// <summary>
		/// Provides a Logger scoped to this controller.
		/// </summary>
		protected readonly ILogger _logger;
		protected readonly IMemoryCache _cache;

		public MultimediaController(IOptions<ConnectionStringConfig> configAccessor, ILogger<CourseController> logger, IMemoryCache cache)
		{
			_logger = logger;
			_logger.LogInformation("Started MultimediaController");
			_cache = cache;
		}

		//https://meta.discourse.org/t/official-single-sign-on-for-discourse-sso/13045
		[HttpGet("vimeo/{vimeoId}")]
		public async Task<ActionResult> GetVimeoQualities(long vimeoId)
		{
			_logger.LogDebug("Entering GetMM {}", vimeoId);
			//debug the information
			_logger.LogDebug("GetMM: multimediaId {}", vimeoId);

			var client = new VimeoClient(Startup.VimeoAccessToken);
			var video = await client.GetVideoAsync(vimeoId);
			_logger.LogDebug("Got video {} with {} files", video?.Id, video?.Files?.Count);

			return Ok(
				(video.Files ?? new List<File>())
				.Where(f => new[]{ 360, 1024, 720 }.Contains(f.Height))
				.Select(f => {
					var signature = new Regex("[?&]s(ignature)?=(?<signature>[^&]+)")
						.Match(f.Link)
						.Groups["signature"]?.Value;
					var token = new Regex("[?&]oauth2_token_id?=(?<token>[^&]+)")
						.Match(f.Link)
						.Groups["token"]?.Value;
					return new AltVideoResolution($"{f.Height}p", signature, token);
				})
				.Aggregate(new Dictionary<string, AltVideoResolution>(), (acc, cur) => {
					acc.Add(cur.Resolution, cur);
					return acc;
				})
			);
		}

		[HttpPost("vimeo/alt-resolutions")]
		public async Task<IActionResult> GetVimeoQualitiesMulti([FromBody] string[] vimeoIds)
		{
			List<long> numericVimeoIds;
			try
			{
				numericVimeoIds = vimeoIds.Select(x => Int64.Parse(x)).ToList();
			}
			catch (Exception e)
			{
				_logger.LogError(e.ToString());
				throw;
			}

			List<AltVideoResolutions> result = new List<AltVideoResolutions>();

			var client = new VimeoClient(Startup.VimeoAccessToken);
			foreach (long vimeoId in numericVimeoIds)
			{
				Dictionary<string, AltVideoResolution> resolutions;
				resolutions = GetAltVideoResolutionsFromCache(vimeoId);
				if (resolutions == null)
				{
					Video video = await client.GetVideoAsync(vimeoId);
					Dictionary<string, AltVideoResolution> o = (video.Files ?? new List<File>())
					.Where(f => new[] { 360, 1024, 720 }.Contains(f.Height))
					.Select(f =>
					{
						var signature = new Regex("[?&]s(ignature)?=(?<signature>[^&]+)")
							.Match(f.Link)
							.Groups["signature"]?.Value;
						var token = new Regex("[?&]oauth2_token_id?=(?<token>[^&]+)")
							.Match(f.Link)
							.Groups["token"]?.Value;
						return new AltVideoResolution($"{f.Height}p", signature, token);
					})
					.Aggregate(new Dictionary<string, AltVideoResolution>(), (acc, cur) =>
					{
						acc.Add(cur.Resolution, cur);
						return acc;
					});

					_cache.Set(Startup.CachePrefix.VIMEO_VIDEO + vimeoId, o);

					resolutions = o;
				}

				result.Add(new AltVideoResolutions(vimeoId, resolutions));
			}

			return Ok(result);
		}


		private Dictionary<string, AltVideoResolution> GetAltVideoResolutionsFromCache(long vimeoId)
		{
			if (this._cache.TryGetValue(Startup.CachePrefix.VIMEO_VIDEO + vimeoId, out Dictionary<string, AltVideoResolution> video))
				return video;
			else
				return null;
		}
	}

	internal class AltVideoResolutions
	{
		public long vimeo_id { get; set; }
		public Dictionary<string, AltVideoResolution> resolutions { get; set; }

		public AltVideoResolutions(long vimeo_id, Dictionary<string, AltVideoResolution> resolutions)
		{
			this.vimeo_id = vimeo_id;
			this.resolutions = resolutions;
		}
	}

	internal class AltVideoResolution
	{
		public string Resolution { get; set; }
		public string Signature { get; set; }
		public string Token { get; set; }

		public AltVideoResolution(string resolution, string signature, string token)
		{
			Resolution = resolution;
			Signature = signature;
			Token = token;
		}
	}
}


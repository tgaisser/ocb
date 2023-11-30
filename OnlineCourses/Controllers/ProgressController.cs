using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Hillsdale.OnlineCourses.Controllers
{

	/// <summary>
	/// Provides the base for any Controller. This handles setting up a logger, connection strings, and a UserInformation instance.
	/// </summary>
	[Route("courses/{courseId}/progress")]
	[Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
	[ApiController]
	public class ProgressController : Controller
	{
		/// <summary>
		/// Provides all available connection strings
		/// </summary>
		protected readonly ConnectionStringConfig _connectionStringConfig;


		/// <summary>
		/// Provides a Logger scoped to this controller.
		/// </summary>
		protected readonly ILogger _logger;

		private CourseData _courseData;


		public ProgressController(IOptions<ConnectionStringConfig> configAccessor, ILogger<CourseController> logger)
		{
			_connectionStringConfig = configAccessor.Value;
			_logger = logger;
			// _logger.LogInformation("Started ProgressController");
			_courseData = new CourseData(_connectionStringConfig, logger);
		}

		[HttpGet("")]
		[HttpGet("new")]
		public async Task<Progress> GetNew(string courseId)
		{

			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			return (await _courseData.GetCoursesProgress(userId, courseId, true)).FirstOrDefault();
		}

		[HttpPut]
		public async Task<bool> MarkCourseOpen(string courseId)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			if (userId == null) return false;

			return await _courseData.MarkCourseOpen(userId, courseId);
		}


		[HttpPost("files/{fileType}")]
		public async Task<bool> MarkFileDownload(string courseId, FileDownloadType fileType, [FromBody] string url)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			return userId != null && await _courseData.MarkFileDownload(userId, courseId, null, url, fileType);
		}

		[HttpPut("lecture/{lectureId}")]
		public async Task<bool> MarkLectureOpen(string courseId, string lectureId)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			if (userId == null) return false;

			return await _courseData.MarkLectureOpen(userId, courseId, lectureId);
		}

		//		[HttpPost("lecture/{lectureId}")]
		//		public bool MarkQAOpen(string courseId, string qaId)
		//		{
		//			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
		//
		//			if (userId == null) return false;
		//
		//			_logger.LogInformation("Logging QA ({}) open for {} on {}", qaId, userId, courseId);
		//
		//			return true;
		//		}

		[HttpPost("lecture/{lectureId}/files/{fileType}")]
		public async Task<bool> MarkFileDownload(string courseId, string lectureId, FileDownloadType fileType, [FromBody] string url)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			return userId != null && await _courseData.MarkFileDownload(userId, courseId, lectureId, url, fileType);
		}

		[HttpPut("lecture/{lectureId}/videos/{videoId}")]
		public async Task<decimal> LectureVideoProgress(string courseId, string videoId, string lectureId, [FromBody] VideoPostInfoData[] data)
		{
			decimal progress = 0.0m;

			foreach (var item in data){
				decimal temp = await VideoProgress("lecture", courseId, videoId, lectureId, item.videoPosition, item.eventTime);
				if(temp < 0) return temp;
				progress = Math.Max(progress, temp);
			}

			return progress;
		}

		[HttpPut("qa/{lectureId}/videos/{videoId}")]
		public async Task<decimal> QAVideoProgress(string courseId, string videoId, string lectureId, [FromBody] VideoPostInfoData[] data)
		{
			decimal progress = 0.0m;

			foreach (var item in data){
				decimal temp = await VideoProgress("qa", courseId, videoId, lectureId, item.videoPosition, item.eventTime);
				if(temp < 0) return temp;
				progress = Math.Max(progress, temp);
			}

			return progress;
		}

		public class VideoPostInfoData {
			public decimal videoPosition; // in seconds; has 3 decimal places
			public long eventTime; // epoch time in milliseconds
		}

		public async Task<decimal> VideoProgress(string type, string courseId, string videoId, string lectureId, decimal videoPosition, long eventTime)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
			
			if (userId == null){
				this.Response.StatusCode = 401;
				return -1;
			}

			return await _courseData.MarkVideoProgress(type, userId, courseId, videoId, lectureId, videoPosition, eventTime);
		}

		[HttpPut("{type:alpha}/{lectureId}/videos/{videoId}/bulk")]
		public async Task<ActionResult<decimal?>> VideoBulkProgress(string type, string courseId, string videoId, string lectureId, [FromBody] WatchInfo[] watches)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
			
			if (userId == null){
				this.Response.StatusCode = 401;
				return -1;
			}

			if (type != "lecture" && type != "qa")
			{
				return BadRequest();
			}

			_logger.LogDebug("Bulk Video Save for {0}: {1}", videoId, watches.Select(s => $"{s.Start.Pos} -> {s.End.Pos}: {s.End.Time}").ToArray());
			//await Task.CompletedTask;
			//return Ok(1);
			//await VideoProgress(type, courseId, videoId, lectureId, videoPosition));
			return await _courseData.MarkBulkVideoProgress(type, userId, courseId, videoId, lectureId, watches);
		}

		//		[HttpPut("reading/{videoId}")]
		//		public bool ReadingProgress(string courseId, string readingId, [FromBody] decimal videoPosition)
		//		{
		//			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
		//
		//			if (userId == null) return false;
		//
		//			_logger.LogInformation("Logging reading ({}) progress ({}) for {} on {}", readingId, videoPosition, userId, courseId);
		//
		//			return true;
		//		}

	}

}

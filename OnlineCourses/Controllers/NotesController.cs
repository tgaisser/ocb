using System.Security.Claims;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Hillsdale.OnlineCourses.Controllers
{
	[Route("notes/")]
	[Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
	[ApiController]
	public class NotesController: Controller
	{
		private readonly ConnectionStringConfig _connectionStringConfig;


		/// <summary>
		/// Provides a Logger scoped to this controller.
		/// </summary>
		private readonly ILogger _logger;

		private readonly NotesData _courseData;

		public NotesController(IOptions<ConnectionStringConfig> configAccessor, ILogger<CourseController> logger)
		{
			_connectionStringConfig = configAccessor.Value;
			_logger = logger;
			// _logger.LogInformation("Started NotesController");
			_courseData = new NotesData(_connectionStringConfig, logger);
		}

//		[HttpGet("{noteId}")]
//		public ActionResult GetNote(string courseId, string lectureId)
//		{
//			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
//
//			if (userId == null) return StatusCode(401, "Must be logged in to store or access notes.");
//
//			_logger.LogDebug("Entering GetNote {}, {}, {}", courseId, lectureId, quizId);
//
//			return Ok(_courseData.GetQuizResult(userId, courseId, lectureId, quizId)); //TODO
//		}

		[HttpGet]
		public async Task<ActionResult> GetNotes()
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			if (userId == null) return StatusCode(401, "Must be logged in to access notes.");

			_logger.LogDebug("Entering GetNotes {}", userId);

			return Ok(await _courseData.GetNotes(userId));
		}

		[HttpGet("headers")]
		public async Task<ActionResult> GetNoteHeaders()
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			if (userId == null) return StatusCode(401, "Must be logged in to access notes.");

			_logger.LogDebug("Entering GetNoteHeaders {}", userId);

			return Ok(await _courseData.GetNoteHeaders(userId));
		}


		[HttpGet("{courseId}")]
		public async Task<ActionResult> GetNotesForCourse(string courseId)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			if (userId == null) return StatusCode(401, "Must be logged in to access notes.");

			_logger.LogDebug("Entering GetNotesForCourse {}, {}", userId, courseId);

			return Ok(await _courseData.GetNotes(userId, courseId));
		}


		[HttpGet("{courseId}/headers")]
		public async Task<ActionResult> GetNoteHeadersForCourse(string courseId)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			if (userId == null) return StatusCode(401, "Must be logged in to access notes.");

			_logger.LogDebug("Entering GetNoteHeadersForCourse {}, {}", userId, courseId);

			return Ok(await _courseData.GetNoteHeaders(userId, courseId));
		}

		[HttpGet("{courseId}/lectures/{lectureId}")]
		public async Task<ActionResult> GetNote(string courseId, string lectureId)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			if (userId == null) return StatusCode(401, "Must be logged in to access notes.");

			_logger.LogDebug("Entering GetNote {}, {}", userId, courseId);

			return Ok(await _courseData.GetNote(userId, courseId, lectureId));
		}

		[HttpPut("{courseId}/lectures/{lectureId}")]
		public async Task<ActionResult> SaveNote(string courseId, string lectureId, [FromBody] string text)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			if (userId == null) return StatusCode(401, "Must be logged in to save notes.");

			_logger.LogDebug("Entering SaveNote {}, {}", userId, courseId);

			return Ok(await _courseData.SaveNote(userId, courseId, lectureId, text));
		}
	}
}
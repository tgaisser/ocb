using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Threading.Tasks;
using Hillsdale.OnlineCourses.Models;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Hillsdale.OnlineCourses.Controllers
{
	[Route("courses/{courseId}/lecture/{lectureId}/quiz/")]
	[Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
	[ApiController]
	public class QuizController: Controller
	{
		protected readonly ConnectionStringConfig _connectionStringConfig;
		private readonly HubspotCourseEnrollment _hubspot;


		/// <summary>
		/// Provides a Logger scoped to this controller.
		/// </summary>
		protected readonly ILogger _logger;

		private CourseData _courseData;
		private UserData _userData;

		public QuizController(IOptions<ConnectionStringConfig> configAccessor, IOptions<HubspotCourseEnrollment> hubspotAccessor, ILogger<CourseController> logger)
		{
			_connectionStringConfig = configAccessor.Value;
			_logger = logger;
			_hubspot = hubspotAccessor.Value;
			// _logger.LogInformation("Started QuizController");
			_courseData = new CourseData(_connectionStringConfig, logger);
			_userData = new UserData(_connectionStringConfig, logger);
		}

		[HttpGet("{quizId}")]
		public async Task<ActionResult> GetQuizResult(string courseId, string lectureId, string quizId)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			if (userId == null) return StatusCode(401, "Must be logged in to get quiz progress.");

			_logger.LogDebug("Entering GetQuizResult {}, {}, {}", courseId, lectureId, quizId);

			var quizRequest = new QuizResultRequest()
			{
				UserId = userId,
				QuizIdentifier = new QuizIdentifier
				{
					CourseId = courseId,
					LectureId = lectureId,
					QuizId = quizId
				}
			};

			return Ok(await _courseData.GetQuizResult(quizRequest));
		}

		[HttpPut("{quizName}")]
		public async Task<ActionResult> GradeQuiz(
			string courseId,
			string lectureId,
			string quizName,
			Dictionary<string, string> answers,
			[FromHeader(Name = "Authorization")] string authToken
		)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
			var username = HttpContext.User.FindFirst("username")?.Value;
			//var authToken = Request.Headers["Authorization"];
			_logger.LogDebug("Got username {}", username);

			//determine if the course is complete prior to this quiz result (used later to mark completion if this is the deciding result)
			var courseWasComplete = await _courseData.IsCourseComplete(userId, courseId);

			if (userId == null) return StatusCode(401, "Must be logged in to take quizzes.");

			_logger.LogDebug("Entering GradeQuiz");
			try
			{
				var grade = await GetQuizGrade(quizName, answers);

				grade.CourseId = courseId;
				grade.LectureId = lectureId;
				grade.CompleteTime = DateTime.Now;

				await _courseData.MarkQuizGrade(userId, grade);

				Response.OnCompleted(async () =>
				{
					await UpdateHubspotEnrollmentIfComplete(courseId, authToken, userId, courseWasComplete, username);
				});

				// re-query in order to populate value of BestPercentageCorrect
				var quizResultRequest = new QuizResultRequest()
				{
					UserId = userId,
					QuizIdentifier = new QuizIdentifier
					{
						CourseId = grade.CourseId,
						LectureId = grade.LectureId,
						QuizId = grade.QuizId
					}
				};
				grade = await _courseData.GetQuizResult(quizResultRequest);
				grade.LectureId = lectureId;
				return Ok(grade);
			}
			catch (KeyNotFoundException e)
			{
				_logger.LogError(e, "Grading exception");
				return StatusCode(404, e.Message);
			}
			catch (Exception e)
			{
				_logger.LogError(e, "Grading exception");
				return StatusCode(500);
			}
		}

		private async Task UpdateHubspotEnrollmentIfComplete(string courseId, string authToken, string userId, bool courseWasComplete, string username)
		{
			var courseIsComplete = await _courseData.IsCourseComplete(userId, courseId);
			_logger.LogDebug("Course {0} was incomplete ({1}) and is now complete ({2})", courseId, !courseWasComplete,
				courseIsComplete);
			//TODO only run this if the course _wasn't_ complete before
			if (!courseWasComplete && courseIsComplete)
			{
				//handle hubspot enrollment
				try
				{
					var email = await _userData.GetCurrentUserEmail(authToken);
					var courseKey = await _courseData.GetCourseHubspotKey(courseId);
					_logger.LogDebug("Course Complete post: course='{0}', email='{1}'", courseKey, email);
					if (!string.IsNullOrWhiteSpace(courseKey) && !string.IsNullOrWhiteSpace(email))
					{
						var hubspotEnrollSuccessful = await _hubspot.SetUserCourseCompletion(
							email,
							courseKey,
							HttpContext.Connection.RemoteIpAddress
								?.ToString(), //TODO maybe? https://stackoverflow.com/questions/28664686/how-do-i-get-client-ip-address-in-asp-net-core
							HttpContext.Request.Cookies["hubspotutk"],
							null,
							null,
							null,
							null,
							null,
							_logger
						);
						_logger.LogDebug("HubSpot enroll of {} to {} was successful: {}",
							email, courseKey, hubspotEnrollSuccessful);
					}
					else
					{
						_logger.LogDebug(
							"Hubspot enrollment skipped. Course (Id: {}, Key: {}) User (Id: {}, Email: {})",
							courseId, courseKey, userId, email);
					}
				}
				catch (Exception e)
				{
					_logger.LogError(e, "Error posting course completion to hubspot");
				}
			}
		}

		private async Task<QuizResult> GetQuizGrade(string quizName, Dictionary<string, string> answers)
		{
			var quiz = await GetQuiz(quizName);

			var results = new List<QuizAnswerGrade>();

			foreach (var (key, value) in answers)
			{
				var match = quiz.modular_content.Select(a => a.Value).FirstOrDefault(a => a?.system.id == key);
				if (match != null)
				{
					results.Add(new QuizAnswerGrade()
					{
						Id = key,
						Correct = ((string) match?.elements.answer?.parsedValue == value),
						SelectedOption = value,
					});
				}
			}

			var correct = results.Count(r => r.Correct);

			return new QuizResult()
			{
				QuizId = quiz.item.system.id,
				QuizName = quizName,
				Results = results,
				Score = correct,
				NumQuestions = results.Count(),
				//Since .net has no built in way, I'm doing this to force the number DOWN to 2 decimal points (e.g. 0.477272... will round to 0.47)
				PercentageCorrect = Math.Floor(((correct*1.0m) / results.Count()) * 100) / 100
			};
		}

		private async Task<CourseQuiz> GetQuiz(string name)
		{
			using (var client = new HttpClient())
			{
				if (!string.IsNullOrWhiteSpace(Startup.DataAccessKey))
				{
					client.DefaultRequestHeaders.Authorization =
						new AuthenticationHeaderValue("Bearer", Startup.DataAccessKey);
				}

				var url = new Uri($"{Startup.DataRoot}/items/{name}");
				_logger.LogDebug("Calling data API {}", url);
				var response = await client.GetAsync(url);
				string json;
				using (var content = response.Content)
				{
					json = await content.ReadAsStringAsync();
				}
				//_logger.LogDebug("Got quiz response '{}'", json);

				var obj = JsonConvert.DeserializeObject<CourseQuiz>(json);

				if (string.IsNullOrEmpty(obj?.item?.system?.id))
				{
					throw new KeyNotFoundException("Quiz not found");
				}

				return obj;
			}
		}
	}

	public class CourseQuiz
	{
		public KenticoItem item { get; set; }
		//public string quiz_id { get; set; }
		public Dictionary<string, QuizQuestion> modular_content { get; set; }
	}

	public class KenticoItem
	{
		public KenticoSystem system { get; set; }
	}

	public class QuizQuestion
	{
		public KenticoSystem system { get; set; }
		public QuizQuestionElem elements { get; set; }
	}

	public class QuizQuestionElem  {
		public KenticoValue<string> question { get; set; }
		public KenticoValue<string> option_1 { get; set; }
		public KenticoValue<string> option_2 { get; set; }
		public KenticoValue<string> option_3 { get; set; }
		public KenticoValue<string> option_4{ get; set; }
		public KenticoValue<string> option_5  { get; set; }
		public KenticoValue<KenticoMultiValue[]> answer { get; set; }
		public KenticoValue<int?> answer_video_position { get; set; }
	}

	public class KenticoSystem
	{
		public string id { get; set; }
		public string name { get; set; }
		public string codename { get; set; }
		public string language { get; set; }
		public string type { get; set; }
	}

	public class KenticoValue<T>
	{
		public string type { get; set; }
		public T value { get; set; }

		public object parsedValue
		{
			get{
				var t = typeof(T);
				if (t == typeof(KenticoMultiValue[]))
				{
					return (value as KenticoMultiValue[])?[0]?.codename;
				}

				return value;
			}
		}
	}

	public class KenticoMultiValue
	{
		public string name { get; set; }
		public string codename { get; set; }
	}
}

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace Hillsdale.OnlineCourses.Controllers
{

	/// <summary>
	/// Provides the base for any Controller. This handles setting up a logger, connection strings, and a UserInformation instance.
	/// </summary>
	[Route("/courses")]
	[Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
	[ApiController]
	public class CourseController : Controller
	{
		static readonly Dictionary<string, List<string>> enrolledCourses = new Dictionary<string, List<string>>();

		/// <summary>
		/// Provides all available connection strings
		/// </summary>
		private readonly ConnectionStringConfig _connectionStringConfig;


		/// <summary>
		/// Provides a Logger scoped to this controller.
		/// </summary>
		private readonly ILogger _logger;

		private readonly CourseData _courseData;
		private readonly HubspotCourseEnrollment _hubspot;


		public CourseController(IOptions<ConnectionStringConfig> configAccessor, IOptions<HubspotCourseEnrollment> hubspotAccessor, ILogger<CourseController> logger)
		{
			_connectionStringConfig = configAccessor.Value;
			_hubspot = hubspotAccessor.Value;
			_logger = logger;
			// _logger.LogInformation("Started Course Controller");
			_courseData = new CourseData(_connectionStringConfig, logger);
		}


		[HttpGet]
		public async Task<IEnumerable<CourseEnrollment>> Courses()
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			return await _courseData.GetUserCourses(userId);
		}

		[AllowAnonymous]
		[HttpGet("health")]
		public async Task<ActionResult> HealthCheck()
		{
			return await _courseData.CheckCoursesExist() ? Ok("Running") : StatusCode(500, "Cannot connect to DB or missing courses");
		}

		[HttpGet("withdrawal-reasons")]
		public async Task<IEnumerable<Dictionary<string, object>>> GetWithdrawalReasons()
		{
			return await _courseData.GetWithdrawalReasons();
		}

		[HttpGet("progress")]
		[HttpGet("progress-new")]
		public async Task<IEnumerable<Progress>> Get(string courseId)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			return userId == null ? new Progress[0] : await _courseData.GetCoursesProgress(userId);
		}

		[AllowAnonymous]
		[HttpPut("{courseId}/inquiries")]
		public async Task<ActionResult> InquireInCourse(string courseId, [FromBody] CourseInquiryModel inquiry, [FromQueryAttribute] UtmInfo utmInfo)
		{
			_logger.LogDebug("Utm Params : {0}", utmInfo.Stringify());

			var userHasEarlyAccess = Startup.EarlyAccessToken.Equals(inquiry.EarlyAccessToken);

			try
			{
				var inquireResult = await _courseData.MarkCourseInquiry(
					inquiry.Email, courseId,
					userHasEarlyAccess, inquiry.StudyGroupId,
					utmInfo
				);

				return Ok(inquireResult);
			}
			catch (Exception e)
			{
				_logger.LogError(e, "Error in inquiry");
				return StatusCode(500, "Unable to inquire. Please try again later.");
			}
		}


		[HttpPut("{courseId}")]
		public async Task<ActionResult> Enroll(string courseId, [FromBody] CourseEnrollmentModel enrollment, [FromQuery] UtmInfo utmInfo)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
			var email = HttpContext.User.FindFirst(ClaimTypes.Email)?.Value;
			var firstname = HttpContext.User.FindFirst(ClaimTypes.GivenName)?.Value;
			var lastname = HttpContext.User.FindFirst(ClaimTypes.Surname)?.Value;

			_logger.LogDebug("EnrollInCourse:: UserId: {UserId}, CourseId: {CourseId}, Utm Params : {Utms}, Enrollment: {Enrollment}", userId, courseId, utmInfo.Stringify(), enrollment);

			if (userId == null) return StatusCode(401, "Must be logged in to enroll.");

			var userHasEarlyAccess = Startup.EarlyAccessToken.Equals(enrollment.EarlyAccessToken);

			// temporary code for early access stuff for American Left course 
			//const string americanLeftCourseObjectId = "d0f138ae-188d-430a-9899-c113dbee4a9a";
			//const int americanLeftEarlyAccessHubspotListId = 13230;
			//if(courseId == americanLeftCourseObjectId && !userHasEarlyAccess)
			//{
			//	try
			//	{
			//		userHasEarlyAccess = await IsEmailOnHubSpotEarlyAccessList(email, americanLeftEarlyAccessHubspotListId);
			//	}
			//	catch (Exception e)
			//	{
			//		// this is just a random GUID for quickly searching logs for this error
			//		_logger.LogError("2dba6bca-db39-405e-9384-6309681ed7db\n" + e.ToString());
			//	}
			//}


			try
			{
				CourseEnrollment enrollResult;
				if (enrollment?.StudyGroupId == null)
					enrollResult = await _courseData.MarkCourseEnrollment(userId, courseId, enrollment.Enroll, enrollment.WithdrawalReason, userHasEarlyAccess, utmInfo);
				else
					enrollResult = await _courseData.MarkSubEnrollment(userId, courseId, enrollment.Enroll, enrollment.WithdrawalReason, userHasEarlyAccess, enrollment.StudyGroupId, utmInfo);

				if (enrollResult != null)
				{
					Response.OnCompleted(async () =>
					{
						//handle hubspot enrollment
						if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") != "Development")
						{
							await RunHubspotEnrollment(courseId, userId, enrollment, utmInfo, new ContactInfo { Email = email, Firstname = firstname, Lastname = lastname });
						}
					});
				}

				return Ok(enrollResult);
			}
			catch (Exception e)
			{
				_logger.LogError(e, "Error in enrollment");
				return StatusCode(500, "Unable to enroll. Please try again later.");
			}
		}

		#region temp solution to on-platform early access

		[HttpPut("{courseId}/{earlyAccessToken}")]
		public async Task<ActionResult> UpdateEarlyAccessStatus(string courseId, string earlyAccessToken)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
			var email = HttpContext.User.FindFirst(ClaimTypes.Email)?.Value;

			if (userId == null)
				return StatusCode(401, "You are not logged in.");

			var userHasEarlyAccess = Startup.EarlyAccessToken.Equals(earlyAccessToken);

			// if userHasEarlyAccess, update it on the enrollment record
			if (userHasEarlyAccess == true)
				Response.OnCompleted(async () => await _courseData.SetEarlyAccessOnExistingEnrollment(userId, courseId, 1));

			return Ok(userHasEarlyAccess);
		}

		private async Task<bool> IsEmailOnHubSpotEarlyAccessList(string email, int hubspotListId)
		{
			IEnumerable<HubSpotContact> contacts = await GetHubspotContactsByEmailsAsync(new string[] { email });

			if (contacts.Any() == false)
				return false;

			var contactId = contacts.First().id;
			var listMemberships = await GetHubspotListMembershipsByContactIdAsync(contactId);

			return listMemberships.Any(x => x.staticlistid == hubspotListId && x.ismember == true);
		}

		private static readonly Regex id_pattern = new Regex(@"^\d+$");
		private async Task<IEnumerable<Rootobject.ListMemberships>> GetHubspotListMembershipsByContactIdAsync(string id)
		{
			if (!id_pattern.IsMatch(id))
			{
				_logger.LogError($"Invalid id for HubSpot Contact: {id}");
				return null;
			}

			// need to use API v1 in order to get list memberships
			// using &property=  in order to slightly reduce the response size 
			string contactUrl = $"https://api.hubapi.com/contacts/v1/contact/vid/{id}/profile?showListMemberships=true&property=&propertyMode=value_only";
			using (var httpClient = new HttpClient())
			{
				httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _hubspot.PrivateAppToken);
				using (var response = await httpClient.GetAsync(contactUrl))
				{
					if (response.StatusCode == HttpStatusCode.NotFound)
						return null;

					response.EnsureSuccessStatusCode();

					try
					{
						Rootobject contact = JsonConvert.DeserializeObject<Rootobject>(await response.Content.ReadAsStringAsync());
						return contact.listmemberships;
					}
					catch (Exception e)
					{
						this._logger.LogError(e.ToString());
						throw;
					}
				}
			}
		}

		private async Task<IEnumerable<HubSpotContact>> GetHubspotContactsByEmailsAsync(string[] emails)
		{
			HubSpotSearchRequest req = new HubSpotSearchRequest(new HubSpotSearchRequest.Filter("email", "IN", emails));
			// Note: using System.Text.Json here to utilize the Encoder option to prevent HTML-sensitive characters (such as '+') from being stripped out of email addresses.
			//string bodycontent = System.Text.Json.JsonSerializer.Serialize(req, new JsonSerializerOptions() { IgnoreNullValues = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
			string bodycontent = JsonConvert.SerializeObject(req, Formatting.None, new JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
			var body = new StringContent(bodycontent);
			body.Headers.ContentType = new MediaTypeHeaderValue("application/json");
			using (var httpClient = new HttpClient())
			{
				httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _hubspot.PrivateAppToken);
				using (var response = await httpClient.PostAsync($"https://api.hubapi.com/crm/v3/objects/contacts/search", body))
				{
					try
					{
						response.EnsureSuccessStatusCode();
						string searchResponse = await response.Content.ReadAsStringAsync();
						ContactSearchResponse res = JsonConvert.DeserializeObject<ContactSearchResponse>(searchResponse);
						return new List<HubSpotContact>(res.results);
					}
					catch (Exception e)
					{
						this._logger.LogError(e.ToString());
						throw;
					}
				}
			}
		}

		public class HubSpotSearchRequest
		{
			public HubSpotSearchRequest(params Filter[] filters)
			{
				this.filterGroups = new List<FilterGroup> { new FilterGroup(filters) };
			}
			public List<FilterGroup> filterGroups { get; set; }
			public string[] sorts { get; set; }
			public string query { get; set; }
			public string[] properties { get; set; }
			public int? limit { get; set; }
			public int? after { get; set; }
			// adds another filter group to the search using the filters passed in
			public void AddFilterGroup(params Filter[] filters)
			{
				this.filterGroups.Add(new FilterGroup(filters));
			}

			public class FilterGroup
			{
				public FilterGroup(params Filter[] filters)
				{
					this.filters = filters;
				}
				public Filter[] filters { get; set; }
			}

			public class Filter
			{
				public Filter(string propertyName, string @operator, string value) { this.propertyName = propertyName; this.@operator = @operator; this.value = value; }
				public Filter(string propertyName, string @operator, string[] values) { this.propertyName = propertyName; this.@operator = @operator; this.values = values; }
				public string value { get; set; }
				public string[] values { get; set; }
				public string propertyName { get; set; }
				// operators: EQ NEQ LT LTE GT GTE BETWEEN IN NOT_IN HAS_PROPERTY NOT_HAS_PROPERTY CONTAINS_TOKEN NOT_CONTAINS_TOKEN
				public string @operator { get; set; }
			}
		}

		public class ContactSearchResponse
		{
			public int Total { get; set; }
			public HubSpotContact[] results { get; set; }
		}

		public class HubSpotContact
		{
			public string id { get; set; }
			public Dictionary<string, string> properties { get; set; }
			public DateTime createdAt { get; set; }
			public DateTime updatedAt { get; set; }
			public bool archived { get; set; }

			public class Properties
			{
				public DateTime createdate { get; set; }
				public string email { get; set; }
				public string firstname { get; set; }
				public string hs_object_id { get; set; }
				public DateTime lastmodifieddate { get; set; }
				public string lastname { get; set; }
				public string address { get; set; }
				public string city { get; set; }
				public string state { get; set; }
				public string zip { get; set; }
			}
		}




		public class Rootobject
		{
			public int vid { get; set; }
			public int portalid { get; set; }
			public bool iscontact { get; set; }
			public ListMemberships[] listmemberships { get; set; }

			public class ListMemberships
			{
				public int staticlistid { get; set; }
				public int internallistid { get; set; }
				public long timestamp { get; set; }
				public int vid { get; set; }
				public bool ismember { get; set; }
			}
		}

		#endregion

		private async Task RunHubspotEnrollment(string courseId, string userId, CourseEnrollmentModel enrollment, UtmInfo utm, ContactInfo contact)
		{
			try
			{
				var courseKey = await _courseData.GetCourseHubspotKey(courseId);
				if (!string.IsNullOrWhiteSpace(courseKey) && !string.IsNullOrWhiteSpace(contact.Email))
				{
					var hubspotEnrollSuccessful = await _hubspot.SetUserEnrollment(
						contact.Email,
						contact.Firstname,
						contact.Lastname,
						courseKey,
						enrollment.Enroll,
						HttpContext.Connection.RemoteIpAddress
							?.ToString(), //TODO maybe? https://stackoverflow.com/questions/28664686/how-do-i-get-client-ip-address-in-asp-net-core
						HttpContext.Request.Cookies["hubspotutk"],
						utm.Source,
						utm.Medium,
						utm.Content,
						utm.Campaign,
						utm.Term,
						//TODO Add page?*
						_logger
					);
					_logger.LogDebug("HubSpot enroll of {} to {} was successful: {}",
						contact.Email, courseKey, hubspotEnrollSuccessful);
				}
				else
				{
					_logger.LogDebug(
						"Hubspot enrollment skipped. Course (Id: {}, Key: {}) User (Id: {}, Email: {})",
						courseId, courseKey, userId, contact.Email);
				}
			}
			catch (Exception e)
			{
				_logger.LogError(e, "Error posting hubspot enrollment");
			}
		}


		[HttpGet("{courseId}/quizzes")]
		public async Task<ActionResult> GetQuizResult(string courseId)
		{
			var userId = HttpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

			if (userId == null) return StatusCode(401, "Must be logged in to get quiz progress.");

			_logger.LogDebug("Entering GetQuizResults {}", courseId);

			return Ok(await _courseData.GetQuizResults(userId, courseId));
		}
	}
}

public class CourseEnrollmentModel
{
	public int? WithdrawalReason { get; set; }

	[Required]
	public bool Enroll { get; set; }

	public string EarlyAccessToken { get; set; }

	public string StudyGroupId { get; set; }

	public override string ToString()
	{
		return $"{nameof(WithdrawalReason)}: {WithdrawalReason}, {nameof(Enroll)}: {Enroll}, {nameof(EarlyAccessToken)}: {EarlyAccessToken}, {nameof(StudyGroupId)}: {StudyGroupId}";
	}
}

public class CourseInquiryModel
{
	[Required]
	public string Email { get; set; }

	public string EarlyAccessToken { get; set; }

	public string StudyGroupId { get; set; }
}


public class ContactInfo
{
	public string Email { get; set; }
	public string Firstname { get; set; }
	public string Lastname { get; set; }
}

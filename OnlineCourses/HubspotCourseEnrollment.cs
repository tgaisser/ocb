using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Hillsdale.OnlineCourses
{
	class HubspotContact{
		public HubspotContactProperties properties { get; set; }
	}

	class HubspotContactProperties
	{
		public HubSpotContactCourseEnrollment online_courses_enrollment { get; set; }
		public HubSpotContactCourseEnrollment online_courses_completed { get; set; }
	}

	class HubSpotContactCourseEnrollment
	{
		public string value { get; set; }
	}

	enum HubspotCoursesType
	{
		enrollment,
		completed
	}
	class ContactNotFoundException : Exception { }

	public class HubspotCourseEnrollment
	{
		public string PrivateAppToken { get; set; }
		public string ContactCreateUrl { get; set; }
		public string ContactCompleteCourseUrl { get; set; }

		public string ContactRetrieveUrl { get; set; }

		public async Task<bool> SetUserCourseCompletion(string email, string courseKey, string ipAddress, string hutk, string utmSource = null, string utmMedium = null, string utmContent = null, string utmCampaign = null, string utmTerm = null, ILogger logger = null)
		{
			if (courseKey == null) return false;
			string[] currentCourseList;
			try
			{
				currentCourseList = await GetCourses(email, HubspotCoursesType.completed, logger);
				if (currentCourseList.Contains(courseKey))
				{
					//this user already exists and has the expected courses. Don't resubmit
					return false;
				}
			}
			catch (ContactNotFoundException)
			{
				currentCourseList = new string[0];
			}

			var newCourseList = string.Join(";", currentCourseList.Concat(new[] {courseKey}));
			return await CreateUserAndSetCourseProperty(email, HubspotCoursesType.completed, null, null, newCourseList, hutk, ipAddress, utmSource, utmMedium, utmContent, utmCampaign, utmTerm, logger);
		}

		public async Task<bool> SetUserEnrollment(string email, string firstname, string lastname, string courseKey, bool enrolled, string ipAddress, string hutk, string utmSource = null, string utmMedium = null, string utmContent = null, string utmCampaign = null, string utmTerm = null, ILogger logger = null)
		{
			if (enrolled)
			{
				return await AddUserToCourse(email, firstname, lastname, courseKey, ipAddress, hutk, utmSource, utmMedium, utmContent, utmCampaign, utmTerm, logger);
			}
			else
			{
				return await RemoveUserFromCourse(email, firstname, lastname, courseKey, ipAddress, hutk, utmSource, utmMedium, utmContent, utmCampaign, utmTerm, logger);
			}
		}

		public async Task<bool> AddUserToCourse(string userEmail, string firstname, string lastname, string newCourse,  string ipAddress, string hutk, string utmSource, string utmMedium, string utmContent, string utmCampaign, string utmTerm, ILogger logger){
			string newCourseList = null;

			if (newCourse == null) return false;
			string[] currentCourseList;
			try
			{
				currentCourseList = await GetCourses(userEmail, HubspotCoursesType.enrollment, logger);
				if (currentCourseList.Contains(newCourse))
				{
					//this user already exists and has the expected courses. Don't resubmit
					return false;
				}
			}
			catch (ContactNotFoundException)
			{
				currentCourseList = new string[0];
			}

			newCourseList = string.Join(";", currentCourseList.Concat(new[] {newCourse}));
			return await CreateUserAndSetCourseProperty(userEmail, HubspotCoursesType.enrollment, firstname, lastname, newCourseList, hutk, ipAddress, utmSource, utmMedium, utmContent, utmCampaign, utmTerm, logger);
		}

		public async Task<bool> RemoveUserFromCourse(string userEmail, string firstname, string lastname, string courseToRemove, string ipAddress, string hutk, string utmSource, string utmMedium, string utmContent, string utmCampaign, string utmTerm, ILogger logger){
			string newCourseList = null;

			if (courseToRemove == null) return false;
			string[] currentCourseList;
			try
			{
				currentCourseList = await GetCourses(userEmail, HubspotCoursesType.enrollment, logger);
				if (!currentCourseList.Contains(courseToRemove))
				{
					//this user already exists and doesn't have the course requested to be removed. Don't resubmit
					return false;
				}
			}
			catch (ContactNotFoundException)
			{
				//the contact doesn't exist, so we'll just ignore this unsubscribe request (we don't have any courses to set)
				//TODO should we create the base contact anyway?
				return false;
			}

			newCourseList = string.Join(";", currentCourseList.Where(t => t != courseToRemove));
			newCourseList = !string.IsNullOrWhiteSpace(newCourseList) ? newCourseList : ";";
			logger?.LogDebug("about to send new course list: {}", newCourseList);
			return await CreateUserAndSetCourseProperty(userEmail, HubspotCoursesType.enrollment, firstname, lastname, newCourseList, hutk, ipAddress, utmSource, utmMedium, utmContent, utmCampaign, utmTerm, logger);
		}

		private async Task<bool> CreateUserAndSetCourseProperty(string email, HubspotCoursesType type, string firstname, string lastname, string courseList = null, string hubspotCookie = null, string clientIp = null, string utmSource = null, string utmMedium = null, string utmContent = null, string utmCampaign = null, string utmTerm = null, ILogger logger = null)
		{
			logger?.LogDebug("Calling CreateUserAndSetCourseProperty({}, {}, {}, {}...)", email, firstname, lastname, courseList);
			if (string.IsNullOrWhiteSpace(email)) return false;

			string hsContext = null;
			if (hubspotCookie != null)
			{
				var hsContextValues = new Dictionary<string, string>()
				{
					{"hutk", hubspotCookie}
				};
				if (clientIp != null)
				{
					hsContextValues.Add("ipAddress", clientIp);
				}

				hsContext = JsonConvert.SerializeObject(hsContextValues);
			}



			var values = new List<KeyValuePair<string, string>>
			{
				new KeyValuePair<string, string>("email", email)
			};
			if (!string.IsNullOrWhiteSpace(firstname)) values.Add(new KeyValuePair<string, string>("firstname", firstname));
			if (!string.IsNullOrWhiteSpace(lastname)) values.Add(new KeyValuePair<string, string>("lastname", lastname));
			if (!string.IsNullOrWhiteSpace(utmSource)) values.Add(new KeyValuePair<string, string>("utm_source", utmSource));
			if (!string.IsNullOrWhiteSpace(utmMedium)) values.Add(new KeyValuePair<string, string>("utm_medium", utmMedium));
			if (!string.IsNullOrWhiteSpace(utmContent)) values.Add(new KeyValuePair<string, string>("utm_content", utmContent));
			if (!string.IsNullOrWhiteSpace(utmCampaign)) values.Add(new KeyValuePair<string, string>("utm_campaign", utmCampaign));
			if (!string.IsNullOrWhiteSpace(utmTerm)) values.Add(new KeyValuePair<string, string>("utm_term", utmTerm));

			if (courseList != null)
			{
				values.Add(new KeyValuePair<string, string>(type == HubspotCoursesType.enrollment ? "online_courses_enrollment" : "online_courses_completed", courseList));
			}
			if (hsContext != null)
			{
				values.Add(new KeyValuePair<string, string>("hs_context", hsContext));
			}

			// logger?.LogDebug("Hubspot form {0}", JsonConvert.SerializeObject(values));
			var content = new FormUrlEncodedContent(values);

			try
			{
				using (var httpClient = new HttpClient())
				using (var result = await httpClient.PostAsync(
						type == HubspotCoursesType.enrollment ? ContactCreateUrl : ContactCompleteCourseUrl,
						content
					).ConfigureAwait(false))
				{
					logger.LogDebug("Hubspot response: {}", await result.Content.ReadAsStringAsync());
					result.EnsureSuccessStatusCode();
				}

				return true;
			}
			catch (Exception e)
			{
				logger?.LogError(e, "Unable to post new contact");
				return false;
			}
		}

		private async Task<string[]> GetCourses(string email, HubspotCoursesType type, ILogger logger = null)
		{

			var contactUrl = string.Format(
				ContactRetrieveUrl,
				HttpUtility.UrlEncode(email),
				type == HubspotCoursesType.enrollment ? "online_courses_enrollment" : "online_courses_completed"
			);
			logger?.LogDebug("Getting Hubspot Courses: {}", contactUrl);
			using (var httpClient = new HttpClient())
			{
				httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", PrivateAppToken);
				using (var response = await httpClient.GetAsync(contactUrl))
				{
					if (response.StatusCode == HttpStatusCode.NotFound)
						throw new ContactNotFoundException();

					response.EnsureSuccessStatusCode();

					try
					{
						var userRecord =
							JsonConvert.DeserializeObject<HubspotContact>(await response.Content.ReadAsStringAsync());

						var currentCourses = (type == HubspotCoursesType.enrollment ?
													userRecord?.properties?.online_courses_enrollment?.value :
													userRecord?.properties?.online_courses_completed?.value
												) ?? "";
						return currentCourses.Split(';');
					}
					catch (Exception e)
					{
						throw new FormatException("Invalid JSON format", e);
					}
				}
			}
		}


	}
}

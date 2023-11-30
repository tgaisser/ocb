using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Amazon.Lambda;
using Amazon.Lambda.Model;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;

namespace Hillsdale.OnlineCourses
{
	public class UserData
	{
		private readonly ConnectionStringConfig _connStrConfig;
		private ILogger _logger;

		public UserData(ConnectionStringConfig sqlConfig, ILogger logger)
		{
			_connStrConfig = sqlConfig;
			_logger = logger;
		}

		public async Task<bool> MergeAccounts(string cognitoToken, IEnumerable<UserMatch> matches, IAmazonLambda lambda)
		{
			return await RunMergeFuncOnAccounts(cognitoToken, "merge", matches, lambda);
		}

		public async Task<bool> IgnoreAccounts(string cognitoToken, IEnumerable<UserMatch> matches, IAmazonLambda lambda)
		{
			return await RunMergeFuncOnAccounts(cognitoToken, "ignore", matches, lambda);
		}

		private async Task<bool> RunMergeFuncOnAccounts(string cognitoToken, string action, IEnumerable<UserMatch> matches, IAmazonLambda lambda)
		{
			var user = await GetCurrentUser(cognitoToken);
			var matchedAccountsJson = user.UserAttributes.Find(a => a.Name == "custom:matched_accounts");
			var matchedAccounts = JsonConvert.DeserializeObject<CognitoMatchedAccount[]>(matchedAccountsJson?.Value ?? "[]");

			//get all of the user ids from the passed account list
			//NOTE: when posted to the API, the `UserId` field is the GUID (i.e. `sub`) of the Cognito user. It should not be confused with the `userId` field on `custom:matched_accounts`.
			var requestedMergeIds = matches.Select(m => m.UserId);

			//get all of the subs that are on the matched accounts (plus the one we're operating on)
			var allowedMergeIds = matchedAccounts
				.Select(ma => ma.sub)
				.Append(user.UserAttributes.Find(a => a.Name == "sub").Value);

			// if any of the users passed don't match a user on the Cognito match list, bail
			if (requestedMergeIds.Any(m => !allowedMergeIds.Contains(m)))
			{
				_logger.LogInformation(
					"Some users in ({0}) are not in matched list ({1})",
					requestedMergeIds,
					allowedMergeIds
				);
				return false;
			}

			//trigger lambda function!!!
			try
			{
				_logger.LogInformation("UserData.RunMergeFuncOnAccounts ({0}): UserIds verified", action);
				var response = await lambda.InvokeAsync(new InvokeRequest()
				{
					FunctionName = Startup.MergeAccountFunction,
					Payload = JsonConvert.SerializeObject(new Dictionary<string, object>()
					{
						{"action", action},
						{"userIds", requestedMergeIds},
					}),
					InvocationType = InvocationType.RequestResponse,
				});
				using (var r = new StreamReader(response.Payload))
				{
					var stringResp = await r.ReadToEndAsync();
					_logger.LogDebug("Merge Function ({0}) Response: {1}", Startup.MergeAccountFunction, stringResp);

					return Convert.ToBoolean(stringResp);
				}
			}
			catch (Exception e)
			{
				_logger.LogError(e, "Error calling/parsing lambda function");
				return false;
			}
		}


		public async Task<CognitoUser> GetCurrentUser(string authHeader)
		{
			_logger.LogInformation("Called GetCurrentUser('TOKEN')");

			var token = authHeader.Replace("Bearer ", string.Empty);

			var requestBody = new StringContent(
				JsonConvert.SerializeObject(new Dictionary<string, string>()
				{
					{"AccessToken", token}
				}),
				Encoding.UTF8,
				"application/x-amz-json-1.1"
			);

			try
			{
				using (var httpClient = new HttpClient())
				{
					// httpClient.DefaultRequestHeaders.Add("Content-Type","application/x-amz-json-1.1");
					httpClient.DefaultRequestHeaders.Add("x-amz-target","AWSCognitoIdentityProviderService.GetUser");
					using (var result = await httpClient.PostAsync(
						"https://cognito-idp.us-east-1.amazonaws.com",
						requestBody
					).ConfigureAwait(false))
					{
						var response = await result.Content.ReadAsStringAsync();
						_logger.LogDebug("Cognito response: {}", response);
						result.EnsureSuccessStatusCode();

						return JsonConvert.DeserializeObject<CognitoUser>(response);
					}
				}
			}
			catch (Exception e)
			{
				_logger.LogError(e, "Unable to post new contact");
				return null;
			}
		}

		public async Task<string> GetCurrentUserEmail(string authHeader)
		{
			_logger.LogInformation("Called GetCurrentUserEmail('TOKEN')");


			return (await GetCurrentUser(authHeader))?
				.UserAttributes?
				.FirstOrDefault(t => t.Name == "email")?
				.Value;
		}

		// public async Task<UserSettings> PatchUserSettings(string userId) {


		public async Task<UserSettings> GetUserSettings(string userId)
		{
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				await connection.OpenAsync();
				return await GetUserSettings(userId, connection);
			}
		}
		public async Task<UserSettings> GetUserSettings(string userId, MySqlConnection connection)
		{
			using (var selectCmd = new MySqlCommand("select * from UserSettings where UserId = @UserId", connection))
			{
				if (connection.State != ConnectionState.Open)
				{
					await connection.OpenAsync();
				}

				selectCmd.Parameters.AddWithValue("@UserId", userId);

				using (var reader = await selectCmd.ExecuteReaderAsync())
				{
					var settings = new UserSettings()
					{
						UserId = userId,
					};

					if (await reader.ReadAsync())
					{
						settings.ProgressReportFrequency = reader["ProgressEmailFrequency"]?.ToString();
						settings.EmailStatus = reader["EmailStatus"]?.ToString();
						settings.PreferAudioLectures = reader["PreferAudioLectures"] as bool? ?? false;
						settings.DataSaver = reader["DataSaver"] as bool? ?? false;
						settings.LastUpdate = reader["LastUpdateDate"] as DateTime? ?? DateTime.Now;
					}
					else
					{
						// users don't automatically get a UserPreferences record so until they save preferences, these are the default vals
						settings.ProgressReportFrequency = "Monthly";
						settings.EmailStatus = "Active";
						settings.PreferAudioLectures = false;
						settings.DataSaver = false;
						settings.LastUpdate = DateTime.Now;
					}

					return settings;
				}
			}
		}

		public async Task<UserSettings> SetUserSettings(UserSettings settings)
		{
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				await connection.OpenAsync();
				return await SetUserSettings(settings, connection);
			}
		}

		public async Task<UserSettings> SetUserSettings(UserSettings settings, MySqlConnection connection)
		{
			const string insert = @"
				insert into UserSettings (UserId, ProgressEmailFrequency, PreferAudioLectures, DataSaver, SubjectPreference, EmailStatus, LastUpdateDate)
				values (@UserId, @Frequency, @PreferAudioLectures, @DataSaver, @SubjectPreference, @Status, current_timestamp)
				on duplicate key update ProgressEmailFrequency = @Frequency,
										PreferAudioLectures = @PreferAudioLectures,
										DataSaver = @DataSaver,
										SubjectPreference = @SubjectPreference,
										EmailStatus = @Status,
										LastUpdateDate = current_timestamp
			";

			using (var command = new MySqlCommand(insert, connection))
			{
				if (connection.State != ConnectionState.Open)
				{
					await connection.OpenAsync();
				}

				command.Parameters.AddWithValue("@UserId", settings.UserId);
				command.Parameters.AddWithValue("@Frequency", settings.ProgressReportFrequency);
				command.Parameters.AddWithValue("@PreferAudioLectures", settings.PreferAudioLectures ?? false);
				command.Parameters.AddWithValue("@DataSaver", settings.DataSaver ?? false);
				command.Parameters.AddWithValue("@SubjectPreference", settings.SubjectPreference);
				command.Parameters.AddWithValue("@Status", settings.EmailStatus ?? "Active");

				await command.ExecuteNonQueryAsync();
			}

			return settings;
		}
		public async Task<UserSettings> UpdateUserSettingsWithSubject(string userId, string subject)
		{
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				await connection.OpenAsync();
				var pref = await GetUserSettings(userId);
				pref.SubjectPreference = subject;
				pref.LastUpdate = new DateTime();
				return await SetUserSettings(pref, connection);
			}
		}
		public async Task<UserSettings> UpdateUserSettingsWithPreferAudio(string userId, bool preferAudio)
		{
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				await connection.OpenAsync();
				var pref = await GetUserSettings(userId);
				pref.PreferAudioLectures = preferAudio;
				pref.LastUpdate = new DateTime();
				return await SetUserSettings(pref, connection);
			}
		}

		public async Task SetUserUtmCodes(string userId, string email, string username, string analytics)
		{

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var command = new MySqlCommand("call RecordSignupAnalytics (@UserId, @Email, @Username, @Analytics);", connection))
			{
				await connection.OpenAsync();

				command.Parameters.AddWithValue("@UserId", userId);
				command.Parameters.AddWithValue("@Email", email);
				command.Parameters.AddWithValue("@Username", username);
				command.Parameters.AddWithValue("@Analytics", analytics);

				await command.ExecuteNonQueryAsync();
			}
		}

	}

	public class CognitoUser
	{
		public string Username { get; set; }
		public List<CognitoUserAttribute> UserAttributes { get; set; }
	}


	public class CognitoUserAttribute
	{
		public string Name { get; set; }
		public string Value { get; set; }
	}

	public class UserMatch {
		public string Type { get; set; }
		public string UserId { get; set; }
		public string Email { get; set; }
		public string Status { get; set; }
	}

	public class CognitoMatchedAccount
	{
		public string sub { get; set; }
		public string email { get; set; }
		public string state { get; set; }
		public string userId { get; set; }
	}

	public class UserSettings
	{
		public string UserId { get; set; }
		public string ProgressReportFrequency { get; set; } = "Weekly";

		public bool? PreferAudioLectures { get; set; }
		public bool? DataSaver { get; set; }
		public string SubjectPreference { get; set; }
		public string EmailStatus { get; set; } = "Active";
		public DateTime? LastUpdate { get; set; }

		public override string ToString()
		{
			return $"{nameof(UserId)}: {UserId}, {nameof(ProgressReportFrequency)}: {ProgressReportFrequency}, {nameof(PreferAudioLectures)}: {PreferAudioLectures}, {nameof(SubjectPreference)}: {SubjectPreference}, {nameof(EmailStatus)}: {EmailStatus}, {nameof(LastUpdate)}: {LastUpdate}";
		}
	}
}

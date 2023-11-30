using System;
using System.Collections.Generic;
using System.Data;
//using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Data.Common;
using Hillsdale.OnlineCourses.Models;

namespace Hillsdale.OnlineCourses
{
	public class CourseData
	{
		private readonly ConnectionStringConfig _connStrConfig;
		private readonly ILogger _logger;

		private static readonly Regex VideoIdPattern = new Regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", RegexOptions.IgnoreCase);

		public CourseData(ConnectionStringConfig sqlConfig, ILogger logger)
		{
			_connStrConfig = sqlConfig;
			_logger = logger;
		}

		public async Task<bool> CheckCoursesExist()
		{
			const string courseCheck = @"select count(*) from Courses where DeactivateDate is null";

			try
			{
				using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
				using (var select = new MySqlCommand(courseCheck, connection))
				{
					await connection.OpenAsync();
					return ((long) await select.ExecuteScalarAsync()) > 0;
				}
			}
			catch (Exception e)
			{
				_logger.LogError(e, "Error checking that courses exist");
				return false;
			}
		}

		public async Task<CourseEnrollment> MarkCourseEnrollment(
			string userId, string courseId,
			bool enrolled, int? withdrawalReason,
			bool earlyAccess = false, UtmInfo utm = null
		)
		{
			string debugStatement = $"MarkCourseEnrollment:: call RecordCourseEnrollment ('{userId ?? "NULL"}', '{courseId ?? "NULL"}', {(enrolled ? 1 : 0)}, {(withdrawalReason.HasValue ? withdrawalReason.Value.ToString() : "NULL")}, {(earlyAccess ? 1 : 0)}, '{utm?.Stringify()}', NULL);";
			_logger.LogDebug(debugStatement);
			const string insert = "call RecordCourseEnrollment (@UserId, @CourseId, @Enrolled, @WithdrawalReason, @IsEarlyAccess, @Analytics, @EnrollmentDateOverride);";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var insertCmd = new MySqlCommand(insert, connection))
			{
				if (connection.State == ConnectionState.Closed)
					await connection.OpenAsync();

				insertCmd.Parameters.AddWithValue("@UserId", userId);
				insertCmd.Parameters.AddWithValue("@CourseId", courseId);
				insertCmd.Parameters.AddWithValue("@Enrolled", enrolled ? 1 : 0);
				insertCmd.Parameters.AddWithValue("@WithdrawalReason", (object)withdrawalReason ?? DBNull.Value);
				insertCmd.Parameters.AddWithValue("@IsEarlyAccess", earlyAccess);
				insertCmd.Parameters.AddWithValue("@Analytics", (object)utm?.Stringify() ?? DBNull.Value);
				insertCmd.Parameters.AddWithValue("@EnrollmentDateOverride", DBNull.Value);
				try
				{
					try
					{
						using (var reader = await insertCmd.ExecuteReaderAsync())
						{
							CourseEnrollment ce;
							if (await reader.ReadAsync())
							{
								ce = new CourseEnrollment()
								{
									Id = reader["Id"] as int? ?? 0,
									UserId = userId,
									CourseId = reader["CourseId"].ToString(),
									EnrollmentDate = reader["EnrollmentDate"] as DateTime? ?? DateTime.Now,
									WithdrawalDate = reader["WithdrawalDate"] as DateTime?,
									UserHasEarlyAccess = reader["IsEarlyAccess"] as bool? ?? false,
									StudyGroupId = reader["StudyGroupId"] as string,
								};

								return ce;
							}
							else
							{
								_logger.LogError("Didn't receive anything back from DB. Investigate.\n" + debugStatement);
								return null;
							}
						}
					}
					catch (MySqlException e)
					{
						// if it's a MySqlException, log this extra stuff:
						_logger.LogError($"Message={e.Message}\nHResult={e.HResult}");
						_logger.LogError($"Number={e.Number}\nSqlState={e.SqlState}");
						_logger.LogError($"TargetSite={e.TargetSite}\nStackTrace={e.StackTrace}");
						throw;
					}
				}
				catch (Exception e)
				{
					string enrollOrWithdraw = enrolled ? "enroll in" : "withdraw from";
					_logger.LogError(e, $"Unable to {enrollOrWithdraw} course. userId={userId}. courseId={courseId}");
					throw new Exception($"Unable to {enrollOrWithdraw} course. Please try again later.");
				}
				
			}
		}

		private List<string> GetFieldNames(DbDataReader reader){
			var columns = new List<string>();
			for (int i = 0; i < reader.FieldCount; i++)
				columns.Add(reader.GetName(i));
			// _logger.LogError("COLUMNS: " + String.Join(',', columns));
			return columns;
		}

		private async Task<int?> GetScalarFromWhicheverResultSetItIsIn(DbDataReader reader, string columnName)
		{
			object val = null;
			while (await reader.ReadAsync() && val == null)
				if (GetFieldNames(reader).Contains(columnName))
					val = reader[columnName];

			while (await reader.NextResultAsync() && val == null)
				if (await reader.ReadAsync())
					if (GetFieldNames(reader).Contains(columnName))
						val = reader[columnName];

			int? retVal = null;

			if (val is DBNull || val == null)
				return retVal;

			if (val is long)
				return (int?)(long)val;
			else if (val is ulong)
				return (int?)(ulong)val;
			else if (val is int)
				return (int?)(int)val;
			else if (val is uint)
				return (int?)(uint)val;

				_logger.LogError("Scalar is a type that we don't handle in this method :(");
			return -1;
		}

		public async Task<CourseEnrollment> MarkSubEnrollment(
			string userId, string courseId,
			bool enrolled, int? withdrawalReason,
			bool earlyAccess = false, string studyGroupId = null,
			UtmInfo utm = null
		)
		{
			string debugStatement = $"MarkSubEnrollment:: call RecordSubEnrollment ('{userId ?? "NULL"}', '{courseId ?? "NULL"}', {(enrolled ? 1 : 0)}, {(withdrawalReason.HasValue ? withdrawalReason.Value.ToString() : "NULL")}, {(earlyAccess ? 1 : 0)}, '{studyGroupId ?? "NULL"}', '{utm?.Stringify()}', NULL);";
			_logger.LogDebug(debugStatement);
			//update/insert handles creating new record to preserve history
			const string insert = "call RecordSubEnrollment (@UserId, @CourseId, @Enrolled, @WithdrawalReason, @IsEarlyAccess, @StudyGroupId, @Analytics, @EnrollmentDateOverride);";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var insertCmd = new MySqlCommand(insert, connection))
			{
				if (connection.State == ConnectionState.Closed)
					await connection.OpenAsync();

				insertCmd.Parameters.AddWithValue("@UserId", userId);
				insertCmd.Parameters.AddWithValue("@CourseId", courseId);
				insertCmd.Parameters.AddWithValue("@Enrolled", enrolled ? 1 : 0);
				insertCmd.Parameters.AddWithValue("@WithdrawalReason", (object)withdrawalReason ?? DBNull.Value);
				insertCmd.Parameters.AddWithValue("@IsEarlyAccess", earlyAccess);
				insertCmd.Parameters.AddWithValue("@StudyGroupId", studyGroupId);
				insertCmd.Parameters.AddWithValue("@Analytics", (object)utm?.Stringify() ?? DBNull.Value);
				insertCmd.Parameters.AddWithValue("@EnrollmentDateOverride", DBNull.Value);
				try
				{
					try
					{
						using (var reader = await insertCmd.ExecuteReaderAsync())
						{
							CourseEnrollment ce;
							if (await reader.ReadAsync())
							{
								ce = new CourseEnrollment()
								{
									Id = reader["Id"] as int? ?? 0,
									UserId = userId,
									CourseId = reader["CourseId"].ToString(),
									EnrollmentDate = reader["EnrollmentDate"] as DateTime? ?? DateTime.Now,
									WithdrawalDate = reader["WithdrawalDate"] as DateTime?,
									UserHasEarlyAccess = reader["IsEarlyAccess"] as bool? ?? false,
									StudyGroupId = reader["StudyGroupId"] as string,
								};

								return ce;
							}
							else
							{
								_logger.LogError("SE: Didn't receive anything back from DB. Investigate.\n" + debugStatement);
								return null;
							}
						}
					}
					catch (MySqlException e)
					{
						// if it's a MySqlException, log this extra stuff:
						_logger.LogError($"Message={e.Message}\nHResult={e.HResult}");
						_logger.LogError($"Number={e.Number}\nSqlState={e.SqlState}");
						_logger.LogError($"TargetSite={e.TargetSite}\nStackTrace={e.StackTrace}");
						throw;
					}
				}
				catch (Exception e)
				{
					string enrollOrWithdraw = enrolled ? "enroll in" : "withdraw from";
					_logger.LogError(e, $"Unable to {enrollOrWithdraw} study group. userId={userId}. studyGroupId={studyGroupId}");
					throw new Exception($"Unable to {enrollOrWithdraw} study group. Please try again later.");
				}
			}
		}

		public async Task<CourseInquiry> MarkCourseInquiry(
			string userEmail, string courseId, bool earlyAccess = false, string studyGroupId = null,
			UtmInfo utm	= null
		)
		{
			//update/insert handles creating new record to preserve history
			const string insert = "call RecordInquiry (@UserEmail, @CourseId, @IsEarlyAccess, @StudyGroupId, @Analytics);";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var insertCmd = new MySqlCommand(insert, connection))
			{
				if (connection.State == ConnectionState.Closed)
				{
					await connection.OpenAsync();
				}

				insertCmd.Parameters.AddWithValue("@UserEmail", userEmail);
				insertCmd.Parameters.AddWithValue("@CourseId", courseId);
				insertCmd.Parameters.AddWithValue("@IsEarlyAccess", earlyAccess);
				insertCmd.Parameters.AddWithValue("@StudyGroupId", studyGroupId);
				insertCmd.Parameters.AddWithValue("@Analytics", (object) utm?.Stringify() ?? DBNull.Value);

				try
				{
					using (var reader = await insertCmd.ExecuteReaderAsync())
					{
						await reader.ReadAsync();

						return new CourseInquiry()
						{
							Id = reader["Id"] as int? ?? 0,
							CourseId = reader["CourseId"] as string,
							Email = reader["Email"].ToString(),
							InquiryDate = reader["InquiryDate"] as DateTime? ?? DateTime.Now,
							UserHasEarlyAccess = reader["IsEarlyAccess"] as bool? ?? false,
							StudyGroupId = reader["StudyGroupId"] as string,
						};

					}
				}
				catch (Exception e)
				{
					_logger.LogError(e, $"Unable to inquire in course.  userEmail={userEmail}. courseId={courseId}");
					throw new Exception("Unable to inquire in course. Please try again later.");
				}
			}
		}

		public async Task SetEarlyAccessOnExistingEnrollment(string userId, string courseId, int isEarlyAccess)
		{
			if (isEarlyAccess != 1)
				isEarlyAccess = 0;

			const string sql = @"UPDATE CourseEnrollment SET IsEarlyAccess = @IsEarlyAccess WHERE UserId = @UserId AND courseId = @CourseId AND WithdrawalDate IS NULL;";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var sqlCommand = new MySqlCommand(sql, connection))
			{
				if (connection.State == ConnectionState.Closed)
					await connection.OpenAsync();

				sqlCommand.Parameters.AddWithValue("@UserId", userId);
				sqlCommand.Parameters.AddWithValue("@CourseId", courseId);
				sqlCommand.Parameters.AddWithValue("@IsEarlyAccess", isEarlyAccess);

				try
				{
					int recordsAffected = await sqlCommand.ExecuteNonQueryAsync();
					if (recordsAffected > 1)
					{
						_logger.LogError("WARNING: We would expect this to only update at most 1 record.");
					}
					else if (recordsAffected == 0)
					{
						_logger.LogError("WARNING: User should not have been able to make this request if not enrolled in the course.");
					}
				}
				catch (Exception e)
				{
					_logger.LogError(e, $"Unable to update early access status userId={userId}. courseId={courseId}");
				}
			}
		}

		public async Task<IEnumerable<Dictionary<string, object>>> GetWithdrawalReasons()
		{
			var reasons = new List<Dictionary<string, object>>();

			const string withdrawalCheckSql = @"
				select	Id,
						Reason
				from	CourseWithdrawalReasons
				where	DeactivateDate is null";
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var withdrawalCheck = new MySqlCommand(withdrawalCheckSql, connection))
			{
				await connection.OpenAsync();

				using (var reader = await withdrawalCheck.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						reasons.Add(new Dictionary<string, object>() {
							{"id", reader["Id"] as int? ?? 0},
							{"text", reader["Reason"] as string}
						});
					}
				}
			}

			return reasons;
		}
		public async Task<IEnumerable<CourseEnrollment>> GetUserCourses(string userId)
		{
			if (userId == null) return new CourseEnrollment[] { };

			var enrollments = new List<CourseEnrollment>();

			const string query = @"
SELECT ce.Id,
       ce.CourseId,
       ce.EnrollmentDate,
       ce.WithdrawalDate,
       ce.IsEarlyAccess,
       se.StudyGroupId
FROM CourseEnrollment ce
         LEFT OUTER JOIN SubEnrollment se ON ce.Id = se.CourseEnrollmentId AND se.EndDate IS NULL
WHERE UserId = @UserId
  AND WithdrawalDate IS NULL
			";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var command = new MySqlCommand(query, connection))
			{
				command.Parameters.AddWithValue("@UserId", userId);

				await connection.OpenAsync();
				using (var reader = await command.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						enrollments.Add(new CourseEnrollment()
						{
							Id = reader["Id"] as int? ?? 0,
							UserId = userId,
							CourseId = reader["CourseId"].ToString(),
							EnrollmentDate = reader["EnrollmentDate"] as DateTime? ?? DateTime.Now,
							WithdrawalDate = reader["WithdrawalDate"] as DateTime?,
							UserHasEarlyAccess = reader["IsEarlyAccess"] as bool? ?? false,
							StudyGroupId = reader["StudyGroupId"] as string,
						});
					}
				}
			}

			return enrollments;
		}
		//public async Task<Dictionary<string, bool>> CheckForEarlyAccess(string userEmail, IEnumerable<string> courseIdsToCheck)
		//{
		//	// 1. call hubspot for contact record - tell it to include contact associations in the response.
		//	// 2. call database to get EarlyAccessListIds from Courses table
		//	// 3. match up the results to determine which of the requested courses this user has early access for
		//	// 4. return a dictionary that maps courseId -> bool, indicating which courses the user has early access


		//}
		public async Task<CourseEnrollment> GetCourseEnrollmentRecord(int courseEnrollmentId)
		{
			var enrollments = new List<CourseEnrollment>();

			const string query = @"
SELECT ce.UserId,
       ce.CourseId,
       ce.EnrollmentDate,
       ce.WithdrawalDate,
       ce.IsEarlyAccess,
       se.StudyGroupId
FROM CourseEnrollment ce
         LEFT OUTER JOIN SubEnrollment se ON ce.Id = se.CourseEnrollmentId AND se.EndDate IS NULL
WHERE ce.Id = @courseEnrollmentId
			";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var command = new MySqlCommand(query, connection))
			{
				command.Parameters.AddWithValue("@courseEnrollmentId", courseEnrollmentId);

				await connection.OpenAsync();
				using (var reader = await command.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						enrollments.Add(new CourseEnrollment()
						{
							Id = courseEnrollmentId,
							UserId = reader["UserId"].ToString(),
							CourseId = reader["CourseId"].ToString(),
							EnrollmentDate = reader["EnrollmentDate"] as DateTime? ?? DateTime.Now,
							WithdrawalDate = reader["WithdrawalDate"] as DateTime?,
							UserHasEarlyAccess = reader["IsEarlyAccess"] as bool? ?? false,
							StudyGroupId = reader["StudyGroupId"] as string,
						});
					}
				}
			}

			if (enrollments.Any() == false)
				return null;
			else
				return enrollments.First();
		}
		public async Task<CourseEnrollment> GetCourseEnrollmentRecordBySubEnrollmentId(int subEnrollmentId)
		{
			var enrollments = new List<CourseEnrollment>();

			const string query = @"
SELECT ce.UserId,
       ce.CourseId,
       ce.EnrollmentDate,
       ce.WithdrawalDate,
       ce.IsEarlyAccess,
       se.StudyGroupId
FROM CourseEnrollment ce
         INNER JOIN SubEnrollment se ON ce.Id = se.subEnrollmentId
WHERE se.Id = @subEnrollmentId
			";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var command = new MySqlCommand(query, connection))
			{
				command.Parameters.AddWithValue("@subEnrollmentId", subEnrollmentId);

				await connection.OpenAsync();
				using (var reader = await command.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						enrollments.Add(new CourseEnrollment()
						{
							Id = reader["Id"] as int? ?? 0,
							UserId = reader["UserId"].ToString(),
							CourseId = reader["CourseId"].ToString(),
							EnrollmentDate = reader["EnrollmentDate"] as DateTime? ?? DateTime.Now,
							WithdrawalDate = reader["WithdrawalDate"] as DateTime?,
							UserHasEarlyAccess = reader["IsEarlyAccess"] as bool? ?? false,
							StudyGroupId = reader["StudyGroupId"] as string,
						});
					}
				}
			}

			if (enrollments.Any() == false)
				return null;
			else
				return enrollments.First();
		}

		private async Task<List<Progress>> GetCoursesOverviewProgress(MySqlConnection connection, string userId, string courseId = null)
		{
			try
			{
				var courses = new List<Progress>();

			const string query = @"
				select uci.UserId,
					uci.CourseId,
					c.Name CourseName,
					c.InstructionHours,
					uci.EnrollmentDate,
					uci.LastActivityDate,
					uci.Completed,
					uci.CompleteDate,
					uci.Progress as ProgressPercentage,
					if(uci.Progress > 0, 1, 0) as Started
				from UserCourseInfo uci,
					Courses c
				where uci.UserId = @UserId
				  and uci.WithdrawalDate is null
				  and (@CourseId is null or @CourseId = uci.CourseId)
				  and c.DeactivateDate is null
				  and c.ObjId = uci.CourseId;";

			using (var command = new MySqlCommand(query, connection))
			{
				command.Parameters.AddWithValue("@UserId", userId);
				command.Parameters.AddWithValue("@CourseId", courseId ?? (object) DBNull.Value);

				using (var reader = await command.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						if (reader["CourseId"].ToString() == "91dd7a61-b02b-4284-8fc8-b9eaa5251bb1")
						{
							_logger.LogDebug("course progress {0}", reader["ProgressPercentage"]);
							_logger.LogDebug("unparsed: {}, raw: {}, parsed: {}", reader["Completed"].GetType().Name, reader["Completed"] as long?, (reader["Completed"] as int? ?? 0) == 1);
						}
						courses.Add(new Progress
							{
								CourseId = reader["CourseId"]?.ToString(),
								ItemType = "Course",
								ItemId = reader["CourseId"]?.ToString(),
								ItemName = reader["CourseName"]?.ToString(),
								Children = new List<Progress>(),
								Completed = reader["Completed"] as bool? ?? false,
								ProgressPercentage = (reader["ProgressPercentage"] as decimal? ?? 0.0m) / 100,
								LastActivityDate = reader["LastActivityDate"] as DateTime?,
								Started = (reader["Started"] as long? ?? 0) == 1,
								CompletedDate = reader["CompleteDate"] as DateTime?,
								InstructionHours = reader["InstructionHours"] as decimal?
						}
						);
					}
				}
			}

				return courses;
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				throw;
			}
		}

		public async Task<IEnumerable<Progress>> GetCoursesProgress(string userId, string courseId = null, bool includeVideoDetail = false)
		{
			try
			{
				const string query = @"
				with userCourses as (
					select	uci.UserId,
							uci.CourseId
					from	UserCourseInfo uci,
							Courses c
					where uci.UserId = @UserId
					  and uci.WithdrawalDate is null
					  and (@CourseId is null or @CourseId = uci.CourseId)
					  and c.DeactivateDate is null
					  and c.ObjId = uci.CourseId
				),
				userCourseElems as (
					select  userCourses.CourseId,
							userCourses.UserId,
							ce.ElemType,
							ce.ElemId
					from    CourseElems ce,
							userCourses
					where   ce.CourseId = userCourses.CourseId
					  and   ce.ElemType in ('quiz', 'final-quiz', 'lecture')
					  and   ce.DeactivateDate is null
				)
				select	userCourseElems.CourseId,
						userCourseElems.ElemType			as ItemType,
						userCourseElems.ElemId				as ItemId,
						l.Name								as ItemName,
						v.Name								as VideoName,
						v.Id								as VideoId,
						if(ip.ProgressPercentage > 0, 1, 0)	as started,
						ip.LastValue						as EndPosition,
						ip.LastActivityDate					as LastActivityDate,
						coalesce(ip.AdjustedProgress, 0)	as ProgressPercentage,
						coalesce(ip.Completed, 0)			as Completed
				from	userCourseElems
				join	Lectures l
					on	userCourseElems.ElemType in ('lecture')
					and	l.ObjId = userCourseElems.ElemId
					and	l.DeactivateDate is null
				left join ItemProgress ip
					on	ip.ItemId = l.ObjId
					and	ip.ItemType = 'lecture'
					and	ip.UserId = userCourseElems.UserId
				left join MediaItems v
					on v.Id = l.MediaId

				union

				select	userCourseElems.CourseId,
						userCourseElems.ElemType			as ItemType,
						userCourseElems.ElemId				as ItemId,
						q.Name								as ItemName,
						''									as VideoName,
						''									as VideoId,
						if(ip.ProgressPercentage > 0, 1, 0)	as started,
						0									as EndPosition,
						ip.LastActivityDate					as LastActivityDate,
						coalesce(ip.AdjustedProgress, 0)	as ProgressPercentage,
						coalesce(ip.Completed, 0)			as Completed
				from	userCourseElems
				join	Quizzes q
					on	q.ObjId = userCourseElems.ElemId
					and	userCourseElems.ElemType in ('quiz', 'final-quiz')
					and	q.DeactivateDate is null
				left join ItemProgress ip
					on	ip.ItemId = q.ObjId
					and	ip.ItemType in ('quiz', 'final-quiz')
					and	ip.UserId = userCourseElems.UserId;";

				using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
				{
					await connection.OpenAsync();

					var courses = await GetCoursesOverviewProgress(connection, userId, courseId);
					var itemProgresses = new List<Progress>();
					var videoProgress = new List<VideoWatchStatus>();

					using (var command = new MySqlCommand(query, connection))
					{
						command.Parameters.AddWithValue("@UserId", userId);
						command.Parameters.AddWithValue("@CourseId", courseId ?? (object)DBNull.Value);

						//handle the possibility of duplicate entries (The current ItemProgress implementation leaves the possibility for duplicates.)
						var existingItemIds = new List<string>();
						using (var reader = await command.ExecuteReaderAsync())
						{
							while (await reader.ReadAsync())
							{
								var curCourseId = reader["CourseId"]?.ToString();
								var type = reader["ItemType"]?.ToString();

								var itemId = reader["ItemId"].ToString();

								//if this is a duplicate of one we've already processed, just skip this loop
								if (existingItemIds.Contains(itemId)) continue;
								existingItemIds.Add(itemId);

								itemProgresses.Add(new Progress()
								{
									CourseId = curCourseId,
									ItemType = type == "lecture" ? "Lecture" : (type == "final-quiz" ? "FinalQuiz" : "Quiz"),
									ItemId = itemId,
									ItemName = reader["ItemName"]?.ToString(),
									Started = (reader["started"] as long? ?? 0) == 1,
									ProgressPercentage = (reader["ProgressPercentage"] as long? ?? 0) / 100.0m,
									LastActivityDate = reader["LastActivityDate"] as DateTime?,
									Completed = (reader["Completed"] as long? ?? 0) == 1,
									CompletedDate = (reader["Completed"] as long? ?? 0) == 1 ? reader["LastActivityDate"] as DateTime? : null,
								});

								if (type == "lecture")
								{
									videoProgress.Add(new VideoWatchStatus
									{
										CourseId = curCourseId,
										VideoId = reader["VideoId"]?.ToString(),
										Position = (reader["EndPosition"] as long? ?? 0),
										ModifiedDate = reader["LastActivityDate"] as DateTime? ?? DateTime.Now
									});
								}
							}
						}
					}

					foreach (var course in courses)
					{
						course.Children = itemProgresses.Where(i => i.CourseId == course.CourseId).ToList();

						if (includeVideoDetail)
						{
							course.VideoStatuses = videoProgress.Where(i => i.CourseId == course.CourseId).ToList();
						}
					}
					return courses;
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				throw;
			}
		}

		public async Task<bool> IsCourseComplete(string userId, string courseId)
		{
			using (var conn = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				return await IsCourseComplete(userId, courseId, conn);
			}
		}
		public async Task<bool> IsCourseComplete(string userId, string courseId, MySqlConnection connection)
		{
			const string query = @"
				select	Progress,
						Completed
				from	UserCourseInfo
				where	UserId = @UserId
				  and	CourseId = @CourseId
				limit 1
			";
			using (var command = new MySqlCommand(query, connection))
			{
                command.Parameters.AddWithValue("@CourseId", courseId);
				command.Parameters.AddWithValue("@UserId", userId);

				await connection.OpenAsync();
				using (var reader = await command.ExecuteReaderAsync())
				{
					if (await reader.ReadAsync())
					{
						_logger.LogDebug("Completion result: {}, {}", reader["Progress"], reader["Completed"]);
						return reader["Completed"].ToString() == "True";
					}
					else
					{
						return false;
					}
				}
			}
		}
		
		public async Task<string> GetCourseHubspotKey(string courseId)
		{
			//TODO add key?
			const string query = @"
				select	c.ObjId as CourseId,
						c.HubspotKey
				from	Courses c
				where	c.DeactivateDate is null
				  and	c.ObjId = @CourseId
				order by c.ObjId
				limit 1
			";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				using (var command = new MySqlCommand(query, connection))
				{
					command.Parameters.AddWithValue("@CourseId", courseId);

					await connection.OpenAsync();
					using (var reader = await command.ExecuteReaderAsync())
					{
						if (await reader.ReadAsync())
						{
							return reader["HubspotKey"] as string;
						}
					}
				}
			}

			return null;
		}

		public async Task<IEnumerable<VideoWatchStatus>> GetCourseVideoProgress(string userId, string courseId)
		{
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				await connection.OpenAsync();
				return await GetCourseVideoProgress(userId, courseId, connection);
			}
		}

		private static async Task<IEnumerable<VideoWatchStatus>> GetCourseVideoProgress(string userId, string courseId, MySqlConnection connection)
		{
			var watches = new List<VideoWatchStatus>();

			const string query = @"
				select	l.MediaId as VideoId,
						ip.LastValue as EndPosition,
						ip.LastActivityDate as EndWatchTime
				from	CourseElems ce,
						Lectures l,
						ItemProgress ip
				where	ce.CourseId = @CourseId
				  and	ce.DeactivateDate is null
				  and	ce.ElemType in ('lecture')
				  and	l.ObjId = ce.ElemId
				  and	l.DeactivateDate is null
				  and	ip.UserId = @UserId
				  and	ip.ItemId = l.ObjId
				  and	ip.ItemType = 'lecture'";

			using (var command = new MySqlCommand(query, connection))
			{
				command.Parameters.AddWithValue("@CourseId", courseId);
				command.Parameters.AddWithValue("@UserId", userId);

				if (connection.State == ConnectionState.Closed)
				{
					connection.Open();
				}

				using (var reader = await command.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						watches.Add(new VideoWatchStatus
						{
							VideoId = reader["VideoId"]?.ToString(),
							Position = reader["EndPosition"] as int? ?? 0,
							ModifiedDate = reader["EndWatchTime"] as DateTime? ?? DateTime.Now
						});

					}
				}
			}

			return watches;
		}

		public async Task<QuizResult> GetQuizResult(QuizResultRequest request)
		{
			if (request.UserId == null) return null;

			QuizResult quizResult = null;

			try
			{
				await using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
				{
					await connection.OpenAsync();

					quizResult = await GetLatestQuizResult(connection, request);

					var bestPercentageResult = await GetBestPercentageCorrect(connection, request);

					if (quizResult != null)
					{
						quizResult.BestPercentageCorrect = bestPercentageResult;
					}
				}

			}
			catch (Exception e)
			{
				_logger.LogError(e, "Unable to get quiz result");
			}
			return quizResult;
		}

		private async Task<QuizResult> GetLatestQuizResult(MySqlConnection connection, QuizResultRequest request)
		{
			QuizResult quizResult = null;

			try
			{
				const string query = @"
		        select 
					q.Id
					, q.UserId
					, q.CourseId
					, q.QuizId
					, q.Score
					, q.Percentage
					, q.StartTime
					, q.CompleteTime
					, q.LectureId
					, q.NumQuestions
					, q.Results
		        from    QuizResults q
		        where   q.UserId = @UserId
		          and   q.QuizId = @QuizId
		          and   q.CourseId = @CourseId
		          and   q.LectureId = @LectureId
		        order by q.CompleteTime desc
		        limit 1;
		    ";

				await using var command = new MySqlCommand(query, connection);
				command.Parameters.AddWithValue("@UserId", request.UserId);
				command.Parameters.AddWithValue("@CourseId", request.QuizIdentifier.CourseId);
				command.Parameters.AddWithValue("@LectureId", request.QuizIdentifier.LectureId);
				command.Parameters.AddWithValue("@QuizId", request.QuizIdentifier.QuizId);

				await using var reader = await command.ExecuteReaderAsync();
				if (await reader.ReadAsync())
				{
					quizResult = ReadQuizResult(reader);
				}
			}
			catch (Exception e)
			{
				_logger.LogError(e, "Unable to get latest quiz result");
			}

			return quizResult;
		}

		private async Task<decimal> GetBestPercentageCorrect(MySqlConnection connection, QuizResultRequest request)
		{
			var bestPercentageResult = 0.0m;

			try
			{
				const string query = @"
		        select Percentage AS BestPercentageCorrect
		        from    QuizResults
		        where   UserId = @UserId
		          and   QuizId = @QuizId
		          and   CourseId = @CourseId
		          and   LectureId = @LectureId
		        order by Percentage desc
		        limit 1;
		    ";

				await using var command = new MySqlCommand(query, connection);
				command.Parameters.AddWithValue("@UserId", request.UserId);
				command.Parameters.AddWithValue("@CourseId", request.QuizIdentifier.CourseId);
				command.Parameters.AddWithValue("@LectureId", request.QuizIdentifier.LectureId);
				command.Parameters.AddWithValue("@QuizId", request.QuizIdentifier.QuizId);

				await using var reader = await command.ExecuteReaderAsync();
				if (await reader.ReadAsync())
				{
					bestPercentageResult = reader["BestPercentageCorrect"] as decimal? ?? 0.0m;
				}
			}
			catch (Exception e)
			{
				_logger.LogError(e, "Unable to get best percentage correct");
			}

			return bestPercentageResult;
		}

		private QuizResult ReadQuizResult(IDataRecord reader)
		{
			if (reader == null)
			{
				throw new ArgumentNullException(nameof(reader), "The data reader cannot be null.");
			}

			var quizResult = new QuizResult
			{
				Id = reader["Id"] as int? ?? -1,
				CourseId = reader["CourseId"]?.ToString(),
				LectureId = reader["LectureId"]?.ToString(),
				QuizId = reader["QuizId"]?.ToString(),
				Score = reader["Score"] as int? ?? 0,
				NumQuestions = reader["NumQuestions"] as int? ?? 0,
				PercentageCorrect = reader["Percentage"] as decimal? ?? 0.0m,
				StartTime = reader["StartTime"] as DateTime?,
				CompleteTime = reader["CompleteTime"] as DateTime?,
				Results = null
			};

			var serializedResults = reader["Results"]?.ToString();
			if(serializedResults != null)
			{
				quizResult.Results = JsonConvert.DeserializeObject<List<QuizAnswerGrade>>(serializedResults);
			}

			return quizResult;
		}

		public async Task<List<QuizResult>> GetQuizResults(string userId, string courseId)
		{
			var results = new List<QuizResult>();
			if (userId == null) return results;

			const string select = @"
				with latestQuizzes as (
					select Id, UserId, CourseId, QuizId, Score, Percentage, StartTime, CompleteTime, LectureId, NumQuestions, IsPreQuiz, Results,
					       ROW_NUMBER() over (partition by LectureId, QuizId order by CompleteTime desc, Id) as rowNum
					from	QuizResults
					where	UserId = @UserId
					  and	CourseId = @CourseId
				),
				bestPercentages as (
					select LectureId, QuizId, MAX(Percentage) as BestPercentageCorrect
					from	QuizResults
					where	UserId = @UserId
					  and	CourseId = @CourseId
					group by LectureId, QuizId
				)
				select Id, UserId, CourseId, LQ.QuizId, Score, Percentage, StartTime, CompleteTime, LQ.LectureId, NumQuestions, IsPreQuiz, Results, BP.BestPercentageCorrect
				from	latestQuizzes LQ, bestPercentages BP
				where	LQ.rowNum = 1
				  and	LQ.LectureId = BP.LectureId
				  and	LQ.QuizId = BP.QuizId;";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var command = new MySqlCommand(select, connection))
			{
				command.Parameters.AddWithValue("@CourseId", courseId);
//				command.Parameters.AddWithValue("@LessonId", lessonId);
				command.Parameters.AddWithValue("@UserId", userId);

				await connection.OpenAsync();
				using (var reader = await command.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						//						_logger.LogDebug("perc '{}'", reader["Percentage"]);
						QuizResult quizResult = ReadQuizResult(reader);
						quizResult.BestPercentageCorrect = reader["BestPercentageCorrect"] as decimal? ?? 0.0m;
						results.Add(quizResult);
					}
				}
			}

			return results;
		}

		public async Task<List<Progress>> GetCourseQuizProgress(string userId, string courseId)
		{
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				return await GetCourseQuizProgress(userId, courseId, connection);
			}
		}

		public async Task<List<Progress>> GetCourseQuizProgress(string userId, string courseId, MySqlConnection connection)
		{
			_logger.LogDebug("Running quiz progress for '{}' on course '{}'", userId, courseId);
			var results = new List<Progress>();
			if (userId == null) return results;

			const string select = @"
				with bestQuizzes as (
					select Id, UserId, CourseId, QuizId, Score, Percentage, StartTime, CompleteTime, LectureId, NumQuestions, IsPreQuiz,
					       ROW_NUMBER() over (partition by LectureId, QuizId order by Percentage desc, Id) as rowNum
					from	QuizResults
					where	UserId = @UserId
					  and	CourseId = @CourseId
				)

				select	c.ObjId CourseId,
						c.Name CourseName,
						q.ObjId QuizId,
						q.Name QuizName,
						if(qr.Percentage is not null, 1, 0) as started,
						case (ce.ElemType)
							when 'quiz' then if(qr.Percentage is not null, 1, 0)
							when 'final-quiz' then if(coalesce(qr.Percentage, 0) >= 0.8, 1, 0)
							else 0
						end as Completed,
						coalesce(qr.Percentage, 0) as ProgressPercentage,
						case (ce.ElemType)
							when 'quiz' then 'Quiz'
							when 'final-quiz' then 'FinalQuiz'
							else 'ignore'
						end as ItemType,
						qr.CompleteTime
				from	Quizzes q
				join	CourseElems ce
					on	ce.ElemId = q.ObjId
				   and	ce.DeactivateDate is null
				join	Courses c
					on	c.ObjId = ce.CourseId
				   and	c.DeactivateDate is null
				join	CourseEnrollment e
					on	e.CourseId = c.ObjId
				   and	e.WithdrawalDate is null
				left join bestQuizzes as qr
					on	q.ObjId = qr.QuizId
				   and	qr.rowNum = 1
				where	e.UserId = @UserId
				  and	q.DeactivateDate is null
				  and	(@CourseId is null or @CourseId = c.ObjId)
				group by c.ObjId, c.Name, e.UserId, ce.ElemType, q.ObjId, q.Name, qr.Id, qr.Percentage
				order by c.ObjId
			";

			using (var command = new MySqlCommand(select, connection))
			{
				command.Parameters.AddWithValue("@CourseId", courseId);
//				command.Parameters.AddWithValue("@LessonId", lessonId);
				command.Parameters.AddWithValue("@UserId", userId);

				if (connection.State == ConnectionState.Closed)
				{
					await connection.OpenAsync();
				}


				using (var reader = await command.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						if ("ignore" == reader["ItemType"]?.ToString()) continue;

//						_logger.LogDebug("perc '{}'", reader["Percentage"]);
						results.Add(new Progress
						{
							ItemId = reader["QuizId"]?.ToString(),
							ItemType = reader["ItemType"]?.ToString(),
							ItemName = reader["QuizName"]?.ToString(),
							Started = (reader["started"] as int? ?? 0) == 1,
							Completed = (reader["Completed"] as int? ?? 0) == 1,
							CompletedDate = reader["CompleteTime"] as DateTime?,
							LastActivityDate = reader["CompleteTime"] as DateTime?,
							ProgressPercentage = reader["ProgressPercentage"] as decimal? ?? 0.0m,
						});
					}
				}
			}

			return results;
		}

		public async Task<bool> MarkCourseOpen(string userId, string courseId)
		{
			if (userId == null) return false;

			const string insert = @"
				insert into CourseAccess (UserId, CourseId)
				select	@UserId UserId,
					ObjId CourseId
				from	Courses
				where	ObjId = @CourseId
				  and	DeactivateDate is null
				limit 1;
			";
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var command = new MySqlCommand(insert, connection))
			{
//				command.Parameters.AddWithValue("@CourseId", courseId); //TODO should we stash this here as well?
				command.Parameters.AddWithValue("@CourseId", courseId);
				command.Parameters.AddWithValue("@UserId", userId);

				await connection.OpenAsync();
				await command.ExecuteNonQueryAsync();
			}

			_logger.LogInformation("Logging course ({}) open for {}", userId, courseId);

			//mark this as 1% progress (will be ignored if > 1 already)
			//TODO pull existing type
			await MarkItemProgress(courseId, "course", 1, userId, false);

			return true;
		}


		public async Task<bool> MarkLectureOpen(string userId, string courseId, string lectureId)
		{
			if (userId == null) return false;

			const string insert = @"
				insert into LectureAccess (UserId, CourseId, LectureId, Type)
				select	@UserId UserId,
					@CourseId CourseId,
					ObjId LectureId,
				        Type
				from	Lectures
				where	ObjId = @LectureId
				  and	DeactivateDate is null
				limit 1;
			";
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var command = new MySqlCommand(insert, connection))
			{
//				command.Parameters.AddWithValue("@CourseId", courseId); //TODO should we stash this here as well?
				command.Parameters.AddWithValue("@CourseId", courseId);
				command.Parameters.AddWithValue("@LectureId", lectureId);
				command.Parameters.AddWithValue("@UserId", userId);

				await connection.OpenAsync();
				await command.ExecuteNonQueryAsync();
			}

			_logger.LogInformation("Logging lecture ({}) open for {} on {}", lectureId, userId, courseId);

			//mark this as 1% progress (will be ignored if > 1 already)
			//TODO pull existing type
			await MarkItemProgress(lectureId, "lecture", 1, userId, false);

			return true;
		}

		public async Task<bool> MarkQuizGrade(string userId, QuizResult result)
		{
			if (userId == null) return false;

			const string insert = @"
				insert into QuizResults (UserId, CourseId, LectureId, QuizId, NumQuestions, Score, Percentage, CompleteTime, Results)
				values(@UserId, @CourseId, @LectureId, @QuizId, @NumQuestions, @Score, @Percentage, current_timestamp, @Results);
			";
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var command = new MySqlCommand(insert, connection))
			{
//				command.Parameters.AddWithValue("@CourseId", courseId); //TODO should we stash this here as well?
				command.Parameters.AddWithValue("@CourseId", result.CourseId);
				command.Parameters.AddWithValue("@LectureId", result.LectureId);
				command.Parameters.AddWithValue("@UserId", userId);
				command.Parameters.AddWithValue("@QuizId", result.QuizId);
				command.Parameters.AddWithValue("@NumQuestions", result.NumQuestions);
				command.Parameters.AddWithValue("@Score", result.Score);
				command.Parameters.AddWithValue("@Percentage", result.PercentageCorrect);
				command.Parameters.AddWithValue("@Results", result.Results == null ? DBNull.Value : (object)JsonConvert.SerializeObject(result.Results));

				await connection.OpenAsync();

				await command.ExecuteScalarAsync();

				result.Id = command.LastInsertedId;
				await MarkQuizProgress(result.QuizId, (int) (result.PercentageCorrect * 100), connection, userId);
			}

			_logger.LogInformation("Logging quiz res ({}) for {} in {} - {}", result.PercentageCorrect, result.QuizId, result.CourseId, result.LectureId);

			return true;
		}

		/// <summary>
		/// Mark video watch progress
		/// </summary>
		/// <param name="type">The type of this video. Allowed values: 'lecture' and 'qa'</param>
		/// <param name="userId">The user who watched the video</param>
		/// <param name="courseId">The course the user was on when watching</param>
		/// <param name="videoId">The video the user watched</param>
		/// <param name="lectureId">The lecture the user was on when watching</param>
		/// <param name="videoPosition">The position in the video, expressed as number of seconds from the beginning</param>
		/// <returns>A decimal between 0 and 1 representing the percentage through the video (e.g. 0.5 is 50%)</returns>
		public async Task<decimal> MarkVideoProgress(string type, string userId, string courseId, string videoId, string lectureId, decimal videoPosition, long eventTime)
		{
			//validate the videoId
			if (!VideoIdPattern.IsMatch(videoId))
			{
				_logger.LogWarning("VideoId '{}' is not a valid videoId", videoId);
				return -1;
			}

			_logger.LogInformation("Logging video ({}) progress ({}) for {} on {}", videoId, videoPosition, userId, courseId);

			const string insertSql = @"call RecordVideoProgress(@UserId, @VideoId, @LatestPosition, @CourseId, @LectureId, @LectureType, @EventTime);";

			// Console.WriteLine($"userId={userId}, lectureId={lectureId}, videoId={videoId}, eventTime={DateTimeOffset.FromUnixTimeMilliseconds(eventTime).DateTime.ToString("yyyyMMdd hh:mm:ss")}, videoPosition={videoPosition}");

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var insertCmd = new MySqlCommand(insertSql, connection))
			{
				await connection.OpenAsync();

				insertCmd.Parameters.AddWithValue("@UserId", userId);
				insertCmd.Parameters.AddWithValue("@VideoId", videoId);
				insertCmd.Parameters.AddWithValue("@CourseId", courseId);
				insertCmd.Parameters.AddWithValue("@LectureId", (object) lectureId ?? DBNull.Value);
				insertCmd.Parameters.AddWithValue("@LatestPosition", (int) videoPosition); //round down to the second
				insertCmd.Parameters.AddWithValue("@LectureType", type);
				insertCmd.Parameters.AddWithValue("@EventTime", DateTimeOffset.FromUnixTimeMilliseconds(eventTime).DateTime);

				_logger.LogDebug("call RecordVideoProgress('{UserId}', '{VideoId}', {LatestPosition}, '{CourseId}', '{LectureId}', '{LectureType}', {EventTime})",
					userId, videoId, (int)videoPosition, courseId, lectureId, type, eventTime);
				using (var reader = await insertCmd.ExecuteReaderAsync())
				{
					if (await reader.ReadAsync())
					{
						if (reader["error"] is string err)
						{
							_logger.LogError("Got error from progress insert: {0}", err);
						}
						else
						{
							return reader["progress"] as decimal? ?? -1.0m;
						}
					}

					return -1.0m;
				}
			}
		}


		/// <summary>
		/// Mark video watch progress in batch form (includes start times)
		/// </summary>
		/// <param name="type">The type of this video. Allowed values: 'lecture' and 'qa'</param>
		/// <param name="userId">The user who watched the video</param>
		/// <param name="courseId">The course the user was on when watching</param>
		/// <param name="videoId">The video the user watched</param>
		/// <param name="lectureId">The lecture the user was on when watching</param>
		/// <param name="watches">The List of times and positions for this video</param>
		/// <returns>A decimal representing the percentage through the video (e.g. 0.5 is 50%)</returns>
		public async Task<decimal?> MarkBulkVideoProgress(string type, string userId, string courseId, string videoId, string lectureId, WatchInfo[] watches)
		{

			if (userId == null) return -1;

			//validate the videoId
			if (!VideoIdPattern.IsMatch(videoId))
			{
				_logger.LogWarning("VideoId '{}' is not a valid videoId", videoId);
				return -1;
			}


			_logger.LogInformation("Logging video ({}) progress ({} items) for {} on {}", videoId, watches.Length, userId, courseId);

			const string insertSql = @"call RecordBulkVideoProgress(@UserId, @VideoId, @CourseId, @LectureId, @LectureType, @Progress);";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var insertCmd = new MySqlCommand(insertSql, connection))
			{
				await connection.OpenAsync();

				var progressStr = JsonConvert.SerializeObject(watches);

				insertCmd.Parameters.AddWithValue("@UserId", userId);
				insertCmd.Parameters.AddWithValue("@VideoId", videoId);
				insertCmd.Parameters.AddWithValue("@CourseId", courseId);
				insertCmd.Parameters.AddWithValue("@LectureId", (object) lectureId ?? DBNull.Value);
				insertCmd.Parameters.AddWithValue("@Progress", progressStr); //round down to the second
				insertCmd.Parameters.AddWithValue("@LectureType", type); //round down to the second

				_logger.LogDebug("call RecordBulkVideoProgress('{UserId}', {VideoId}, '{CourseId}', '{LectureId}', '{LectureType}', '{Progress}'",
					userId, videoId, courseId, lectureId, type, progressStr);
				using (var reader = await insertCmd.ExecuteReaderAsync())
				{
					if (await reader.ReadAsync())
					{
						if (reader["error"] is string err)
						{
							_logger.LogError("Got error from progress insert: {0}", err);
						}
						else
						{
							return reader["progress"] as decimal? ?? -1.0m;
						}
					}

					return null;
				}
			}
		}



		public async Task<bool> MarkFileDownload(string userId, string courseId, string lectureId, string url, FileDownloadType type)
		{
			const string insetSql = @"
				    insert into UserFileDownloads (UserId, CourseId, LectureId, DownloadUrl, DownloadType)
				    values (@UserId, @CourseId, @LectureId, @DownloadUrl, @DownloadType);
				";
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var insertCmd = new MySqlCommand(insetSql, connection))
			{
				await connection.OpenAsync();

				insertCmd.Parameters.AddWithValue("@UserId", userId);
				insertCmd.Parameters.AddWithValue("@CourseId", courseId);
				insertCmd.Parameters.AddWithValue("@LectureId", (object) lectureId ?? DBNull.Value);
				insertCmd.Parameters.AddWithValue("@DownloadUrl", url); //round down to the second
				insertCmd.Parameters.AddWithValue("@DownloadType", FileDownloadTypeHelper.GetDescription(type));


				return await insertCmd.ExecuteNonQueryAsync() > 0;
			}
		}

		private async Task<bool> MarkItemProgress(string itemId, string itemType, int progress, string userId = null,
			bool overwriteExisting = true)
		{
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				return await MarkItemProgress(itemId, itemType, progress, connection, userId, overwriteExisting);
			}
		}

		private async Task<bool> MarkItemProgress(string itemId, string itemType, int progress, MySqlConnection connection, string userId = null, bool overwriteExisting = true)
		{

			if (userId == null) return false;

			const string update = @"
					update ItemProgress
				    set ProgressPercentage = case
							when @Progress > ProgressPercentage
								then @Progress
							when ProgressPercentage < 100 and @Overwrite = 1
								then @Progress
							else
								ProgressPercentage
						end,
						LastActivityDate = current_timestamp
					where	UserId = @UserId
					  and	ItemType = @ItemType
				      and	ItemId = @ItemId
				";/*
					where Id = @Id";*/

			const string insert = @"
				insert into ItemProgress (UserId, ItemId, ItemType, ProgressPercentage)
				values (@UserId, @ItemId, @ItemType, @Progress);";


			using (var insertCmd = new MySqlCommand(insert, connection))
			using (var updateCmd = new MySqlCommand(update, connection))
			{
				if (connection.State == ConnectionState.Closed)
				{
					await connection.OpenAsync();
				}


				updateCmd.Parameters.AddWithValue("@ItemId", itemId);
				updateCmd.Parameters.AddWithValue("@ItemType", itemType);
				updateCmd.Parameters.AddWithValue("@Progress", progress);
				updateCmd.Parameters.AddWithValue("@UserId", userId);
				updateCmd.Parameters.AddWithValue("@Overwrite", overwriteExisting);
				var numUpdates = await updateCmd.ExecuteNonQueryAsync();

				if (numUpdates != 0) return true;

				insertCmd.Parameters.AddWithValue("@ItemId", itemId);
				insertCmd.Parameters.AddWithValue("@ItemType", itemType);
				insertCmd.Parameters.AddWithValue("@Progress", progress);
				insertCmd.Parameters.AddWithValue("@UserId", userId);
				insertCmd.Parameters.AddWithValue("@Overwrite", overwriteExisting);

				return (await insertCmd.ExecuteNonQueryAsync() > 0);

			}
		}


		private async Task<bool> MarkQuizProgress(string itemId, int progress, MySqlConnection connection, string userId = null)
		{

			if (userId == null) return false;

			const string update = @"
				update	ItemProgress tgt
				join	CourseElems ce
				  on	ce.ElemId = tgt.ItemId
				  and	ce.ElemType = tgt.ItemType
				  and	ce.DeactivateDate is null
				set		LastActivityDate = if(
							ce.ElemType = 'final-quiz',
							if(@ProgressPercentage > tgt.ProgressPercentage, current_timestamp, tgt.LastActivityDate),
							current_timestamp
						),
						tgt.ProgressPercentage = if(
							ce.ElemType = 'final-quiz',
							if(@ProgressPercentage > tgt.ProgressPercentage, @ProgressPercentage, tgt.ProgressPercentage),
							100
						)
				where	tgt.UserId = @UserId
				  and	tgt.ItemId = @ItemId
			";

			const string insert = @"
				insert into ItemProgress(UserId, ItemId, ItemType, ProgressPercentage)
				select	@UserId as UserId,
						ce.ElemId as ItemId,
						ce.ElemType as ItemType,
						if(
							ce.ElemType = 'final-quiz',
							@ProgressPercentage,
							100
						) as Progress
					from CourseElems ce
					where ce.ElemId = @ItemId
					  and ce.DeactivateDate is null
					limit 1";

			using (var insertCmd = new MySqlCommand(insert, connection))
			using (var updateCmd = new MySqlCommand(update, connection))
			{
				if (connection.State == ConnectionState.Closed)
				{
					await connection.OpenAsync();
				}


				updateCmd.Parameters.AddWithValue("@ItemId", itemId);
				updateCmd.Parameters.AddWithValue("@ProgressPercentage", progress);
				updateCmd.Parameters.AddWithValue("@UserId", userId);
				var numUpdates = await updateCmd.ExecuteNonQueryAsync();

				if (numUpdates != 0) return true;

				insertCmd.Parameters.AddWithValue("@ItemId", itemId);
				insertCmd.Parameters.AddWithValue("@ProgressPercentage", progress);
				insertCmd.Parameters.AddWithValue("@UserId", userId);

				return (await insertCmd.ExecuteNonQueryAsync() > 0);

			}
		}
	}

	public class CourseEnrollment
	{
		public long Id { get; set; }
		public string UserId { get; set; }
		public string CourseId { get; set; }
		public DateTime EnrollmentDate { get; set; }
		public DateTime? WithdrawalDate { get; set; }
		[JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
		public bool? UserHasEarlyAccess { get; set; }
		[JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
		public string StudyGroupId { get; set; }
		[JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
		public long? WithdrawalReason { get; set; }
	}

	public class CourseInquiry
	{
		public long? Id { get; set; }
		public string Email { get; set; }
		public string CourseId { get; set; }
		public DateTime InquiryDate { get; set; }
		public DateTime EnrollmentDate { get; set; }
		public string EnrolledUserId { get; set; }
		public bool? UserHasEarlyAccess { get; set; }
		[JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
		public string StudyGroupId { get; set; }
	}

	public class Progress
	{
		[JsonIgnore]
		public string CourseId { get; set; }
		public string ItemType { get; set; }
		public string ItemId { get; set; }
		public string ItemName { get; set; }
		public decimal ProgressPercentage { get; set; }
		public bool Started { get; set; }
		public bool Completed { get; set; }
		public decimal? InstructionHours { get; set; }

		[JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
		public DateTime? CompletedDate { get; set; }
		public DateTime? LastActivityDate { get; set; }
		public List<Progress> Children { get; set; }

		[JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
		public IEnumerable<VideoWatchStatus> VideoStatuses { get; set; }

		public Progress Clone()
		{
			return (Progress) MemberwiseClone();
		}
	}

	public class QuizAnswerGrade
	{
		public string Id { get; set; }
		public bool Correct { get; set; }
		public string SelectedOption { get; set; }
	}

	public class UtmInfo
	{
		//TODO changes to `JsonPropertyName("utm_source") in .NET Core 3.0
		[FromQuery(Name = "utm_source")]
		[JsonProperty(PropertyName = "utm_source")]
		public string Source { get; set; }

		[FromQuery(Name = "utm_medium")]
		[JsonProperty(PropertyName = "utm_medium")]
		public string Medium { get; set; }

		[FromQuery(Name = "utm_content")]
		[JsonProperty(PropertyName = "utm_content")]
		public string Content { get; set; }

		[FromQuery(Name = "utm_campaign")]
		[JsonProperty(PropertyName = "utm_campaign")]
		public string Campaign { get; set; }

		[FromQuery(Name = "utm_term")]
		[JsonProperty(PropertyName = "utm_term")]
		public string Term { get; set; }

		[FromQuery(Name = "appeal_code")]
		[JsonProperty(PropertyName = "appeal_code")]
		public string AppealCode { get; set; }

		[FromQuery(Name = "sc")]
		[JsonProperty(PropertyName = "sc")]
		public string SourceCode { get; set; }

		[FromQuery(Name="source_partner")]
		[JsonProperty(PropertyName = "source_partner")]
		public string SourcePartner { get; set; }

		[FromQuery(Name = "gclid")]
		[JsonProperty(PropertyName = "gclid")]
		public string GoogleClickID { get; set; }

		public string Stringify()
		{
			return JsonConvert.SerializeObject(this,
				Formatting.None,
				new JsonSerializerSettings {
					NullValueHandling = NullValueHandling.Ignore
				});
		}
	}

	public class VideoWatchStatus
	{
		[JsonIgnore]
		public string CourseId { get; set; }
		public string VideoId { get; set; }
		public long Position { get; set; }
		public DateTime ModifiedDate { get; set; }
	}

	public class WatchInfo
	{
		public WatchTime Start { get; set; }
		public WatchTime End { get; set; }
	}

	public class WatchTime {
		public decimal Pos { get; set; }
		public DateTime Time { get; set; }
	}

//	public class CourseProgress

	public enum FileDownloadType
	{
		Reading,
		StudyGuide,
		Other,
		Audio
	}

	public static class FileDownloadTypeHelper
	{
		public static string GetDescription(FileDownloadType type)
		{
			switch (type)
			{
				case FileDownloadType.Reading: return "Reading";
				case FileDownloadType.StudyGuide: return "Study Guide";
				case FileDownloadType.Other: return "Other";
				case FileDownloadType.Audio: return "Audio";
				default: return null;
			}
		}
		public static FileDownloadType GetFromDescription(string type)
		{
			switch (type)
			{
				case "Reading": return FileDownloadType.Reading;
				case "Study Guide": return FileDownloadType.StudyGuide;
				case "Other": return FileDownloadType.Other;
				case "Audio": return FileDownloadType.Audio;
				default: return FileDownloadType.Other;
			}
		}
	}
}

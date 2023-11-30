using System;
using System.Collections.Generic;
using System.Data;
using MySql.Data.MySqlClient;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Hillsdale.OnlineCourses
{
	public class NotesData
	{
		private ConnectionStringConfig _connStrConfig;
		private ILogger _logger;
		public NotesData(ConnectionStringConfig sqlConfig, ILogger logger)
		{
			_connStrConfig = sqlConfig;
			_logger = logger;
		}

		public async Task<IEnumerable<Note>> GetNotes(string userId, string courseId = null)
		{
			var rawNotes = await GetRawNotes(userId, courseId);
			return rawNotes.Select(n =>
			{
				n.Text = DecryptNote(n.Text);
				return n;
			});

		}

		public async Task<Note> GetNote(string userId, string courseId, string lectureId)
		{
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				await connection.OpenAsync();
				return await GetNote(userId, courseId, lectureId, connection);
			}
		}

		public async Task<Note> GetNote(string userId, string courseId, string lectureId, MySqlConnection connection)
		{
			var note = await GetRawNote(userId, courseId, lectureId, connection);
			if (note != null)
			{
				note.Text = DecryptNote(note.Text);
			}

			return note;
		}

		public async Task<Note> GetRawNote(string userId, string courseId, string lectureId)
		{
			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			{
				await connection.OpenAsync();
				return await GetRawNote(userId, courseId, lectureId, connection);
			}
		}
		public async Task<Note> GetRawNote(string userId, string courseId, string lectureId, MySqlConnection connection) {
			if (userId == null) return null;

			const string query = @"
				select	Id,
						CourseId,
						LectureId,
						NoteText,
						CreateDate,
						UpdateDate
				from	Notes
				where	UserId = @UserId
				  and	CourseId = @CourseId
				  and	LectureId = @LectureId
			";

			using (var command = new MySqlCommand(query, connection))
			{
				command.Parameters.AddWithValue("@UserId", userId);
				command.Parameters.AddWithValue("@CourseId", courseId);
				command.Parameters.AddWithValue("@LectureId", lectureId);

				if (connection.State == ConnectionState.Closed)
				{
					await connection.OpenAsync();
				}

				using (var reader = await command.ExecuteReaderAsync())
				{
					if (await reader.ReadAsync())
					{
						return new Note()
						{
							Id = reader["Id"] as long? ?? 0,
							UserId = userId,
							CourseId = reader["CourseId"]?.ToString(),
							LectureId = reader["LectureId"]?.ToString(),
							Text = reader["NoteText"] as string,
							Created = reader["CreateDate"] as DateTime? ?? DateTime.Now,
							Updated = reader["UpdateDate"] as DateTime?
						};
					}

					return null;
				}
			}
		}

		public async Task<IEnumerable<Note>> GetRawNotes(string userId, string courseId = null) {
			var notes = new List<Note>();

			if (userId == null) return notes;


			const string query = @"
				select	Id,
						CourseId,
						LectureId,
						NoteText,
						CreateDate,
						UpdateDate
				from	Notes
				where	UserId = @UserId
				  and	(@CourseId is null or CourseId = @CourseId)
			";

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var command = new MySqlCommand(query, connection))
			{
				command.Parameters.AddWithValue("@UserId", userId);
				command.Parameters.AddWithValue("@CourseId", courseId ?? (object) DBNull.Value);

				await connection.OpenAsync();
				using (var reader = await command.ExecuteReaderAsync())
				{
					while (await reader.ReadAsync())
					{
						notes.Add(new Note()
						{
							Id = reader["Id"] as long? ?? 0,
							UserId = userId,
							CourseId = reader["CourseId"] as string,
							LectureId = reader["LectureId"] as string,
							Text = reader["NoteText"] as string,
							Created = reader["CreateDate"] as DateTime? ?? DateTime.Now,
							Updated = reader["UpdateDate"] as DateTime?
						});
					}
				}
			}

			return notes;
		}

		public async Task<IEnumerable<Note>> GetNoteHeaders(string userId, string courseId = null)
		{
			var rawNotes = await GetRawNotes(userId, courseId);
			return rawNotes.Select(n =>
			{
				n.Text = null;
				return n;
			});
		}

		public async Task<Note> SaveNote(string userId, string courseId, string lectureId, string noteText)
		{
			const string insert = @"
				insert into Notes (CourseId, UserId, LectureId, NoteText, CreateDate, UpdateDate)
				values (@CourseId, @UserId, @LectureId, @NoteText, current_timestamp, current_timestamp)";
			const string update = @"
				update Notes
				set	NoteText = @NoteText,
					UpdateDate = current_timestamp
				where	UserId = @UserId
				  and	CourseId = @CourseId
				  and	LectureId = @LectureId
				";

//			_logger.LogDebug("About to encrypt note (REMOVE!!!): '{}'", noteText);
			var encryptedText = EncryptNote(noteText);
//			_logger.LogDebug("Encrypted note (REMOVE!!!): '{}'", encryptedText);

			//TODO validate that course and lecture exist

			using (var connection = new MySqlConnection(_connStrConfig.OnlineCourse))
			using (var insertCmd = new MySqlCommand(insert, connection))
			using (var updateCmd = new MySqlCommand(update, connection))
			{
				await connection.OpenAsync();

				updateCmd.Parameters.AddWithValue("@UserId", userId);
				updateCmd.Parameters.AddWithValue("@CourseId", courseId);
				updateCmd.Parameters.AddWithValue("@LectureId", lectureId);
				updateCmd.Parameters.AddWithValue("@NoteText", encryptedText);

				if (await updateCmd.ExecuteNonQueryAsync() == 0)
				{
					insertCmd.Parameters.AddWithValue("@UserId", userId);
					insertCmd.Parameters.AddWithValue("@CourseId", courseId);
					insertCmd.Parameters.AddWithValue("@LectureId", lectureId);
					insertCmd.Parameters.AddWithValue("@NoteText", encryptedText);

					var saveSuccessful = await insertCmd.ExecuteNonQueryAsync() > 0;
					if (!saveSuccessful)
					{
						throw new Exception("Unable to save note. Please try again later.");
					}
				}


				return await GetNote(userId, courseId, lectureId, connection);
			}
		}

		private RijndaelManaged GetEncryption()
		{
			var salt = new byte[] { 5, 15, 195, 12, 83, 32, 44, 44, 91, 174 };

			using (var rfc2898DeriveBytes = new Rfc2898DeriveBytes(Startup.NotesKey, salt))
			{
				return new RijndaelManaged()
				{
					Key = rfc2898DeriveBytes.GetBytes(256 / 8),
					Padding = PaddingMode.None
				};
			}
		}

		private (SymmetricAlgorithm, ICryptoTransform) GetEncryptor()
		{
			var algorithm = GetEncryption();
			return (algorithm, algorithm.CreateEncryptor());//rmCrypto.Key, rmCrypto.IV);
		}
		private (SymmetricAlgorithm, ICryptoTransform) GetDecryptor(byte[] iv)
		{
			var algorithm = GetEncryption();
			//reset the IV to the one passed in
			algorithm.IV = iv;
			return (algorithm, algorithm.CreateDecryptor());//rmCrypto.Key, rmCrypto.IV);
		}

		private string DecryptNote(string encryptedNoteText)
		{
			//_logger.LogDebug("Encrypted (REMOVE) {0}", encryptedNoteText);

			var cypherBytes = Convert.FromBase64String(encryptedNoteText);

			var ivBytes = new byte[16];
			var textBytes = new byte[cypherBytes.Length - 16];
			Array.Copy(cypherBytes, 0, ivBytes, 0, 16);
			Array.Copy(cypherBytes, 16, textBytes, 0, textBytes.Length);

			_logger.LogDebug("all: {0}", string.Join("|", cypherBytes));
			_logger.LogDebug("iv: {0}", string.Join("|", ivBytes));
			_logger.LogDebug("value: {0}", string.Join("|", textBytes));

			var (alg, crypto) = GetDecryptor(ivBytes);
			using(alg)
			using(crypto)
			using(var inStream = new MemoryStream(textBytes))
			using(var cryptStream = new CryptoStream(inStream, crypto, CryptoStreamMode.Read))
			using (var streamReader = new StreamReader(cryptStream))
			{
				return streamReader.ReadToEnd()?.Trim();
			}

		}

		private object EncryptNote(string rawNoteText)
		{
			var textLen = rawNoteText.Length;
			var textMod = textLen % 16;
			var addLen = textMod == 0 ? 0 : 16 - textMod;
			var spacesToAdd = new string(' ', addLen);

			var (alg, crypto) = GetEncryptor();
			using(alg)
			using(crypto)
			using (var outStream = new MemoryStream())
			{
				outStream.Write(alg.IV);

				using (var cryptStream = new CryptoStream(outStream, crypto, CryptoStreamMode.Write))
				using (var streamWriter = new StreamWriter(cryptStream))
				{
					//_logger.LogDebug("raw text (REMOVE): {0}", rawNoteText);
					streamWriter.Write(rawNoteText + spacesToAdd);

					//_logger.LogDebug("outLength (REMOVE): {0}", outStream.Length);
					streamWriter.Flush();
					cryptStream.Flush();


					//_logger.LogDebug("outLength (REMOVE): {0}", outStream.Length);


					return Convert.ToBase64String(outStream.ToArray());
				}
			}
		}
	}

	public class Note
	{
		public long Id { get; set; }
		public string UserId { get; set; }
		public string CourseId { get; set; }
		public string LectureId { get; set; }
		public string LectureName { get; set; }

		[JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
		public string Text { get; set; }
		public DateTime Created { get; set; }
		public DateTime? Updated { get; set; }
	}
}

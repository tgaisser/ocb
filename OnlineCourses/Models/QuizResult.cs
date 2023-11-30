using System;
using System.Collections.Generic;

namespace Hillsdale.OnlineCourses.Models
{
	public class QuizResult
	{
		public long Id { get; set; }
		public string QuizId { get; set; }
		public string QuizName { get; set; }
		public string CourseId { get; set; }
		public string LectureId { get; set; }
		public int NumQuestions { get; set; }
		public int Score { get; set; }
		public decimal PercentageCorrect { get; set; }
		public DateTime? StartTime { get; set; }
		public DateTime? CompleteTime { get; set; }
		public List<QuizAnswerGrade> Results { get; set; }
		public decimal BestPercentageCorrect { get; set; }
	}
}

package workshop

import org.apache.spark.sql.SparkSession

object Ex20GeneratingExamAssessmentReport {

  def main(args: Array[String]): Unit = {

    /**
     * @link https://github.com/jaceklaskowski/spark-workshop/blob/gh-pages/exercises/spark-sql-exercise-Generating-Exam-Assessment-Report.md
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val data = Seq(
      (1, "Question1Text", "Yes", "abcde1", 0, "(x1,y1)"),
      (2, "Question2Text", "No", "abcde1", 0, "(x1,y1)"),
      (3, "Question3Text", "3", "abcde1", 0, "(x1,y1)"),
      (1, "Question1Text", "No", "abcde2", 0, "(x2,y2)"),
      (2, "Question2Text", "Yes", "abcde2", 0, "(x2,y2)")
    ).toDF("Qid", "Question", "AnswerText", "ParticipantID", "Assessment", "GeoTag")

    data.show()

    import org.apache.spark.sql.functions._


    val qid_header = data.withColumn("header", concat(lit("Qid_"), $"Qid"))
    qid_header
      .groupBy("ParticipantID", "Assessment", "GeoTag")
      .pivot("header")
      .agg(first("Qid") as "Qid")
      .show()
  }
}

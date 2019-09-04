package workshop

import org.apache.spark.sql.SparkSession

object Ex11Dates {

  def main(args: Array[String]): Unit = {

    /**
    @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Difference-in-Days-Between-Dates-As-Strings.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dates = Seq(
      "08/11/2015",
      "09/11/2015",
      "09/12/2015").toDF("date_string")
    dates.show()

    /**
     I used ".withColumn("intermediate_date", date_format(unix_timestamp(col("date_string"), "dd/MM/yyyy")
        .cast("timestamp"), "yyyy-MM-dd"))"
     because it always return null
     */
    dates
      .withColumn("intermediate_date", date_format(unix_timestamp(col("date_string"), "dd/MM/yyyy")
        .cast("timestamp"), "yyyy-MM-dd"))
      .withColumn("to_date", to_date($"intermediate_date", "yyyy-MM-dd"))
      .withColumn("today", current_date())
      .withColumn("diff", datediff(col("today"), col("to_date"))).show()
  }
}

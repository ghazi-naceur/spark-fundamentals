package workshop

import org.apache.spark.sql.SparkSession

object Ex18Pivot2ndExample {

  def main(args: Array[String]): Unit = {

    /**
     * @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Using-pivot-for-Cost-Average-and-Collecting-Values.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val data = Seq(
      (0, "A", 223, "201603", "PORT"),
      (0, "A", 22, "201602", "PORT"),
      (0, "A", 422, "201601", "DOCK"),
      (1, "B", 3213, "201602", "DOCK"),
      (1, "B", 3213, "201601", "PORT"),
      (2, "C", 2321, "201601", "DOCK")
    ).toDF("id", "type", "cost", "date", "ship")

    data.show()

    data.groupBy("id", "type")
      .pivot("date")
      .agg(first("cost"))
      .orderBy("id")
      .show()

    data.groupBy("id", "type")
      .pivot("date")
      .agg(first(struct("ship")))
      .orderBy("id")
      .show()
  }
}

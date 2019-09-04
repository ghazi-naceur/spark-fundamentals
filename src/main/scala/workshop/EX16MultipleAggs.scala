package workshop

import org.apache.spark.sql.SparkSession

object EX16MultipleAggs {

  def main(args: Array[String]): Unit = {

    /**
     * @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Multiple-Aggregations.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val nums = spark.range(5).withColumn("group", 'id % 2)
    nums.show()

    nums.groupBy("group")
      .agg("id" -> "max", "id" -> "min", "id" -> "avg")
      .withColumnRenamed("max(id)", "max_id")
      .withColumnRenamed("min(id)", "min_id")
      .withColumnRenamed("avg(id)", "avg_id")
      .show()
  }
}

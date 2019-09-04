package workshop

import org.apache.spark.sql.SparkSession

object Ex14MaxPerGroup {

  def main(args: Array[String]): Unit = {

    /**
     * @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Finding-maximum-values-per-group-groupBy.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val nums = spark.range(5).withColumn("group", 'id % 2)
    nums.show()

    nums.groupBy("group").max("id")
      .withColumnRenamed("max(id)", "max_id").show()
  }
}

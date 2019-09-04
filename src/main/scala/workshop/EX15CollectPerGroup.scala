package workshop

import org.apache.spark.sql.SparkSession

object EX15CollectPerGroup {

  def main(args: Array[String]): Unit = {

    /**
     * @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Collect-values-per-group.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val nums = spark.range(5).withColumn("group", 'id % 2)
    nums.show()

    nums.groupBy("group").agg(collect_list(col("id")))
      .withColumnRenamed("collect_list(id)", "ids")
      .show()
  }
}

package workshop

import org.apache.spark.sql.SparkSession

object Ex30FindingFirstNonNullValuePerGroup {

  def main(args: Array[String]): Unit = {

    /**
     @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Finding-First-Non-Null-Value-per-Group.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val data = Seq(
      (None, 0),
      (None, 1),
      (Some(2), 0),
      (None, 1),
      (Some(4), 1)).toDF("id", "group")

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    val byKeyOrderByOrdering = Window
      .partitionBy("group")
      .orderBy("id")
      .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    import org.apache.spark.sql.functions.first
    val firsts = data.withColumn("first",
      first("id", ignoreNulls = true) over byKeyOrderByOrdering)
        .drop("id")
        .groupBy("group").agg(first(col("first")) as "first_non_null")



    firsts.show()


//    val collect = data.groupBy("group").agg(collect_list(col("id")).as("collection"))
//    collect.show()
//    val result = collect.selectExpr("group", "first(`collection`, false) AS `first_non_null`")
//    result.show()
  }
}

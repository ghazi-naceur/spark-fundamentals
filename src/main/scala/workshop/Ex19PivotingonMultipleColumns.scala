package workshop

import org.apache.spark.sql.SparkSession

object Ex19PivotingonMultipleColumns {

  def main(args: Array[String]): Unit = {

    /**
     * @link https://github.com/jaceklaskowski/spark-workshop/blob/gh-pages/exercises/spark-sql-exercise-Pivoting-on-Multiple-Columns.md
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val data = Seq(
      (100, 1, 23, 10),
      (100, 2, 45, 11),
      (100, 3, 67, 12),
      (100, 4, 78, 13),
      (101, 1, 23, 10),
      (101, 2, 45, 13),
      (101, 3, 67, 14),
      (101, 4, 78, 15),
      (102, 1, 23, 10),
      (102, 2, 45, 11),
      (102, 3, 67, 16),
      (102, 4, 78, 18)).toDF("id", "day", "price", "units")

    data.show()

    import org.apache.spark.sql.functions._

    data
      .groupBy("id").pivot("day")
      .agg(first("price") as "price", first("units") as "unit")
      // .selectExpr("id, 1_price, 2_price, 3_price, 4_price, 1_unit, 2_unit, 3_unit, 4_unit")
      .select("id", "1_price", "2_price", "3_price", "4_price", "1_unit", "2_unit", "3_unit", "4_unit")
      .show()
  }
}

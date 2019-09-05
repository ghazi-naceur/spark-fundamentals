package workshop

import org.apache.spark.sql.SparkSession

object Ex21FlatteningDatasetFromLongToWideFormat {

  def main(args: Array[String]): Unit = {

    /**
     * @link https://github.com/jaceklaskowski/spark-workshop/blob/gh-pages/exercises/spark-sql-exercise-Flattening-Dataset-from-Long-to-Wide-Format.md
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val data = Seq(
      ("k1", "v4", "v7", "d1"),
      ("k1", "v5", "v8", "d2"),
      ("k1", "v6", "v9", "d3"),
      ("k2", "v12", "v22", "d1"),
      ("k2", "v32", "v42", "d2"),
      ("k2", "v11", "v21", "d3")
    ).toDF("key", "val1", "val2", "date")

    data.show()

    import org.apache.spark.sql.functions._


    data
      .groupBy("key")
      .pivot("date")
      .agg(first("val1") as "v1", first("val2") as "v2")
      .orderBy("key")
      .show()
  }
}

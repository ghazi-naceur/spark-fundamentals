package workshop

import org.apache.spark.sql.SparkSession

object Ex26ConvertingArrayToString {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val words = Seq(Array("hello", "world")).toDF("words")
    words.show()

    val result = words.map(row => row.getAs[List[String]]("words").mkString(" ")).withColumnRenamed("value", "solution")
    result.show()

    val result3 = words.withColumn("solution", concat_ws(" ", col("words"))).drop("words")
    result3.show()
  }
}

package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Ex3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val input = List(("1","one,two,three","one"),
      ("2","four,one,five","six"),
      ("3","seven,nine,one,two","eight"),
      ("4","two,three,five","five"),
      ("5","six,five,one","seven")).toDF("id","words","word")

    input.show(false)

    println("Solution 1 :")
    val intermediate1 = input
      .withColumn("split", split(col("words"), ","))
      .drop("words")
      .withColumn("split", explode(col("split")))
        .drop("word")
        .withColumn("list", collect_set(col("id")).over(Window.partitionBy(col("split"))))
      .drop("id").distinct()

    intermediate1.show(false)

    println("Solution 2 :")
    val intermediate2 = input
      .withColumn("split", split(col("words"), ","))
        .withColumn("split", explode(col("split")))
        .drop("words", "word")
        .groupBy("split")
        .agg(collect_set(col("id")))

    intermediate2.show(false)
  }
}

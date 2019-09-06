package workshop

import org.apache.spark.sql.SparkSession

object Ex25CalculatingDifferenceBetweenConsecutiveRows {

  def main(args: Array[String]): Unit = {

    /**
     * @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Calculating-Difference-Between-Consecutive-Rows-Per-Window.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val itDF = spark.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("header", "true")
      .csv("src/main/resources/workshop/it2.csv")
    itDF.show()

    import org.apache.spark.sql.expressions._
    import org.apache.spark.sql.functions._

    val windowSpec = Window.partitionBy("department").orderBy("department","time", "items_sold")
    val result = itDF
      .withColumn("diff", col("running_total") - when((lag("running_total", 1).over(windowSpec)).isNull, 0)
      .otherwise(lag("running_total", 1).over(windowSpec)))
    result.show(false)
  }
}

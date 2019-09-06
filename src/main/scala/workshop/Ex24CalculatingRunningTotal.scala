package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object Ex24CalculatingRunningTotal {

  def main(args: Array[String]): Unit = {

    /**
     * @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Calculating-Running-Total-Cumulative-Sum.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._

    val itDF = spark.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("header", "true")
      .csv("src/main/resources/workshop/it.csv")
    itDF.show()

    val w = Window.partitionBy("department").orderBy("department","time", "items_sold")

    val result = itDF.withColumn("running_total", sum(itDF("items_sold"))
      .over(w))

    result.show()
  }
}

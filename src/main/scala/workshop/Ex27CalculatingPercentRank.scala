package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object Ex27CalculatingPercentRank {

  def main(args: Array[String]): Unit = {

    /**
    @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Calculating-percent-rank.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._

    val employeesDF = spark.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("header", "true")
      .csv("src/main/resources/workshop/employees.csv")
    employeesDF.show()

    val result = employeesDF.withColumn("Percentage", percent_rank over Window.orderBy("Salary"))
      .withColumn("Level", when(col("Percentage") >= 0.7, "High").
        when(col("Percentage") >= 0.4 and col("Percentage") < 0.7, "Average").
        otherwise("Low"))
        .orderBy(desc("Salary"))
    result.show()
  }
}

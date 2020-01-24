package workshop

import org.apache.spark.sql.SparkSession

object Ex9Upper {

  def main(args: Array[String]): Unit = {

    /**
    https://github.com/jaceklaskowski/spark-workshop/blob/gh-pages/exercises/spark-sql-exercise-Using-upper-Standard-Function.md
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._

    val citiesDF = spark.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("header", "true")
      .csv("src/main/resources/workshop/cities.csv")
    citiesDF.show()

    citiesDF.withColumn("upper_city", upper(col("city"))).show()
  }
}

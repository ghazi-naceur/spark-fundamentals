package workshop

import org.apache.spark.sql.SparkSession

object Ex13CalculatingAggs {

  def main(args: Array[String]): Unit = {

    /**
     * @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Finding-maximum-value-agg.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._

    val populationDF = spark.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("header", "true")
      .csv("src/main/resources/workshop/population-sample.csv")
      .withColumn("population", regexp_replace(col("population"), "\\s+", "").cast("integer"))
    populationDF.show()

    populationDF.groupBy("name").max("population").show()

    populationDF.select(max(col("population"))).show()
    populationDF.selectExpr("max(population)").show()
  }
}

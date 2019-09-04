package workshop

import org.apache.spark.sql.SparkSession

object Ex8FindingMostPopulatedCities {

  def main(args: Array[String]): Unit = {

    /**
    @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Finding-Most-Populated-Cities-Per-Country.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._

    val populationDF = spark.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("header", "true")
      .csv("src/main/resources/workshop/population.csv")
      .withColumn("population", regexp_replace(col("population"), "\\s+", "").cast("integer"))
    populationDF.show()

    val populationByCountry = populationDF.groupBy("country").max("population").withColumnRenamed("max(population)", "population")
    populationByCountry.show()

    println("Step 1 :")
    populationByCountry.join(populationDF, "population").show()

    println("Step 2 :")
    populationByCountry.join(populationDF, "population")
      .drop(populationByCountry.col("country"))
      .select("name", "country", "population")
      .show()
  }
}

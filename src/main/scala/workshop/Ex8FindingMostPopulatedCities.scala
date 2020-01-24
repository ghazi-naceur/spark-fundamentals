package workshop

import org.apache.spark.sql.SparkSession

object Ex8FindingMostPopulatedCities {

  def main(args: Array[String]): Unit = {

    /**
    https://github.com/jaceklaskowski/spark-workshop/blob/gh-pages/exercises/spark-sql-exercise-Finding-Most-Populated-Cities-Per-Country.md

     input :
      +-----------------+-------------+----------+
      |             name|      country|population|
      +-----------------+-------------+----------+
      |           Warsaw|       Poland| 1 764 615|
      |           Cracow|       Poland|   769 498|
      |            Paris|       France| 2 206 488|
      |Villeneuve-Loubet|       France|    15 020|
      |    Pittsburgh PA|United States|   302 407|
      |       Chicago IL|United States| 2 716 000|
      |     Milwaukee WI|United States|   595 351|
      +-----------------+-------------+----------+

     output :
      +----------+-------------+----------+
      |      name|      country|population|
      +----------+-------------+----------+
      |    Warsaw|       Poland| 1 764 615|
      |     Paris|       France| 2 206 488|
      |Chicago IL|United States| 2 716 000|
      +----------+-------------+----------+
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

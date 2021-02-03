package workshop

import org.apache.spark.sql.SparkSession

object Ex8 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val input = List(
      ("Warsaw", "Poland", 1764615),
      ("Cracow", "Poland", 769498),
      ("Paris", "France", 2206488),
      ("Villeneuve-Loubet", "France", 15020),
      ("Pittsburgh PA", "United States", 302407),
      ("Chicago IL", "United States", 2716000),
      ("Milwaukee WI", "United States", 595351))
      .toDF("name", "country", "population")

    input.show(false)

    println("Solution 1:")
    val output1 = input.filter(col("population").gt(1000000))
    output1.show(false)

    val intermediate = input.groupBy(col("country"))
      .max("population")
      .withColumnRenamed("max(population)", "population")

    println("intermediate")
    intermediate.show(false)

    val output2 = intermediate.join(input, "population")
      .drop(input.col("country"))
      .select("name", "country", "population")

    output2.show(false)

    /////////////////////////////////////////////
    input.groupBy("country").agg(max("population"))
      .withColumnRenamed("max(population)", "population")
      .join(input.drop("country"), "population")
      .select("name", "country", "population")
      .show(false)
  }
}

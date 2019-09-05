package workshop

import org.apache.spark.sql.SparkSession

object Ex17Pivot {

  def main(args: Array[String]): Unit = {

    /**
     * @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Using-pivot-to-generate-a-single-row-matrix.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dataDF = spark.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("header", "true")
      .csv("src/main/resources/workshop/pivot.csv")
    dataDF.show()

//    dataDF.groupBy("cc").pivot("udate").sum("udate").show()

    val solution = dataDF.groupBy().pivot("udate").agg(first("cc"))
    solution.show()

    val betterSolution = solution.select(lit("cc") as "udate", $"*")
    betterSolution.show()

  }
}

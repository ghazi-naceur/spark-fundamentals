package workshop

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * input :
 * +---+-----------------+-------+
 * | id|             city|country|
 * +---+-----------------+-------+
 * |  0|           Warsaw| Poland|
 * |  1|Villeneuve-Loubet| France|
 * |  2|           Vranje| Serbia|
 * |  3|       Pittsburgh|     US|
 * +---+-----------------+-------+
 *
 * output :
 *
 * +---+-----------------+-------+-----------------+
 * | id|             city|country|       upper_city|
 * +---+-----------------+-------+-----------------+
 * |  0|           Warsaw| Poland|           WARSAW|
 * |  1|Villeneuve-Loubet| France|VILLENEUVE-LOUBET|
 * |  2|           Vranje| Serbia|           VRANJE|
 * |  3|       Pittsburgh|     US|       PITTSBURGH|
 * +---+-----------------+-------+-----------------+
 *
 */
object Ex9 extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import org.apache.spark.sql.functions._
  import spark.implicits._

  val input = List(("0","Warsaw","Poland"),
    ("1","Villeneuve-Loubet","France"),
    ("2","Vranje","Serbia"),
    ("3","Pittsburgh","US")).toDF("id","city","country")

  val output: DataFrame = input.withColumn("upper_city", upper(col("city")))

  input.show(false)
  output.show(false)
}

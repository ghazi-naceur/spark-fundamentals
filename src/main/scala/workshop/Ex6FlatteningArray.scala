package workshop

import org.apache.spark.sql.SparkSession

object Ex6FlatteningArray {

  def main(args: Array[String]): Unit = {

    /**
    https://github.com/jaceklaskowski/spark-workshop/blob/gh-pages/exercises/spark-sql-exercise-Flattening-Array-Columns-From-Datasets-of-Arrays-to-Datasets-of-Array-Elements.md

     input :
      +---------+
      |    value|
      +---------+
      |[a, b, c]|
      |[X, Y, Z]|
      +---------+

     output :
      +---+---+---+
      |  0|  1|  2|
      +---+---+---+
      |  a|  b|  c|
      |  X|  Y|  Z|
      +---+---+---+

     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val input = Seq(
      Seq("a","b","c"),
      Seq("X","Y","Z")).toDF
    input.show()

    input.select((0 until 3).map(i => col("value").getItem(i).as(s"$i")): _*).show

  }
}

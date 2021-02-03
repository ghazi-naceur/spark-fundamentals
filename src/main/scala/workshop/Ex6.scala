package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex6 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val input = Seq(
      Seq("a","b","c"),
      Seq("X","Y","Z")).toDF()

    input.show(false)

    println("Solution :")
    val frame1 = input
      .select((0 to 2).map(index => col("value").getItem(index).as(s"$index")): _*)

    frame1.show(false)
  }
}

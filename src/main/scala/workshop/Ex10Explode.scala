package workshop

import org.apache.spark.sql.SparkSession

object Ex10Explode {

  def main(args: Array[String]): Unit = {

    /**
     @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Using-explode-Standard-Function.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val nums = Seq(Seq(1,2,3)).toDF("nums")
    nums.show()

    import org.apache.spark.sql.functions._
    nums.select(col("nums"), explode(col("nums")).as("num")).show(false)
  }
}

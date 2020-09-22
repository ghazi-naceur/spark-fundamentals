package workshop

import org.apache.spark.sql.SparkSession

object Ex19Pivot3rdExample {

  def main(args: Array[String]): Unit = {

    /**
     * @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Using-pivot-for-Cost-Average-and-Collecting-Values.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val data = Seq(
      (100,1,23,10),
      (100,2,45,11),
      (100,3,67,12),
      (100,4,78,13),
      (101,1,23,10),
      (101,2,45,13),
      (101,3,67,14),
      (101,4,78,15),
      (102,1,23,10),
      (102,2,45,11),
      (102,3,67,16),
      (102,4,78,18)).toDF("id", "day", "price", "units")

//    data.groupBy('id).pivot('day)
//      .agg(F.first('price).alias("price"),F.first('units).alias("unit"))
//    https://stackoverflow.com/questions/45035940/how-to-pivot-on-multiple-columns-in-spark-sql
//    https://stackoverflow.com/questions/42957650/spark-pivot-with-multiple-columns
  }
}

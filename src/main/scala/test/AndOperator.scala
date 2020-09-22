package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object AndOperator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

//    val df1 = List("some value 1").toDF("some field 1")
//    val df2 = List("some value 1").toDF("some field 1")

    val first = List("a","b","c","d","e")
    val second = List("a","b","s","q","e")

    val common = first.intersect(second)

    println(common)

  }
}

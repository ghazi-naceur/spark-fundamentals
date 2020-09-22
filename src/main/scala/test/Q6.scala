package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, sum}

object Q6 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Temp Data").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")


    val spark = SparkSession.builder().getOrCreate()
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val a: Array[Int] = Array(1002, 3001, 4002, 2003, 2002, 3004, 1003, 4006)

    println("This is ap : ")
    val ap = spark.createDataset(a).show()

    val b: DataFrame = spark
      .createDataset(a)
      .withColumn("x", col("value") % 1000)
    println("This is b : ")
    b.show()

    val c = b
      .groupBy(col("x"))
      .agg(count("x"), sum("value"))
    println("This is c : ")
    c.show()

    println("This is d : ")
    val d = c.drop("x")
      .toDF("count", "total")
      .orderBy(col("count").desc, col("total"))
      //    .limit(1)
      .show()
  }
}

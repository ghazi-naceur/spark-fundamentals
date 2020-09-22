package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Q7 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Temp Data").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")


    val spark = SparkSession.builder().getOrCreate()
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val csvDF = spark.read
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .csv("src/main/resources/data.txt")

    csvDF.show()
  }
}

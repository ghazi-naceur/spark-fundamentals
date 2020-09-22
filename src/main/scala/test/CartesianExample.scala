package test

import org.apache.spark.sql.SparkSession

object CartesianExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val a = Seq(
      (1, "arun", "prasanth", 40),
      (2, "ann", "antony", 45),
      (3, "sruthy", "abc", 41)
    )

    val b = Seq(
      (4, "AAA", "prasanth", 40),
      (5, "BBB", "antony", 45),
      (6, "CCC", "antony", 45),
      (7, "DDD", "abc", 41)
    )

    val tableA = spark
      .createDataFrame(a)
      .toDF()

    val tableB = spark
      .createDataFrame(b)
      .toDF()

//    val cartesianRes = tableA.rdd.cartesian(tableB.rdd)
//    cartesianRes.saveAsTextFile("cartesian/cartesian.txt")

    val cartesianFile = spark.read.textFile("cartesian/cartesian.txt")
    cartesianFile.show(30, false)

  }
}

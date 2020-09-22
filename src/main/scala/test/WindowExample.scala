package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowExample {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Temp Data").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val spark = SparkSession.builder().getOrCreate()
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val df = Seq((1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b"))
      .toDF("id", "category")

    val byCategoryOrderedById = Window.partitionBy('category).orderBy('id).rowsBetween(Window.currentRow, 1)

    df.withColumn("sum", sum('id) over byCategoryOrderedById).show()
  }
}

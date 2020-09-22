package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_list, sort_array, struct}

object Q8 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Temp Data").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val spark = SparkSession.builder().getOrCreate()

    val rawData = Seq(
      (1, 1000, "Apple", 0.76),
      (2, 1000, "Apple", 0.11),
      (1, 2000, "Orange", 0.98),
      (1, 3000, "Banana", 0.24),
      (2, 3000, "Banana", 0.99)
    )

    val dfA = spark
      .createDataFrame(rawData)
      .toDF("UserKey", "ItemKey", "ItemName", "Score")

    println("############# 1 :")
    dfA.groupBy("UserKey")
      .agg(collect_list(struct("Score", "ItemKey", "ItemName")))
      .toDF("UserKey", "Collection")
      .show(20, false)

    println("############# 2 :")
    dfA.groupBy("UserKey")
      .agg(sort_array(collect_list(struct("Score", "ItemKey", "ItemName")), false))
      .toDF("UserKey", "Collection")
      .show(20, false)

    println("############# transition to 3 :")
    dfA.groupBy("UserKey", "ItemKey", "ItemName")
      .agg(sort_array(collect_list(struct("Score", "ItemKey", "ItemName")), false)).show()

    println("############# 3 :")
    dfA.groupBy("UserKey", "ItemKey", "ItemName")
      .agg(sort_array(collect_list(struct("Score", "ItemKey", "ItemName")), false))
      .drop("ItemKey", "ItemName")
      .toDF("UserKey", "Collection")
      .show(20, false)

    println("############# transition to 4 :")
    dfA.withColumn(
    "Collection",
        collect_list(struct("Score", "ItemKey", "ItemName"))
          .over(Window.partitionBy("ItemKey"))
    ).show()


//    println("############# transition to 4 bis :")
//    dfA.withColumn(
//      "Collection",
//      collect_list(struct("Score", "ItemKey", "ItemName"))
//    )

    println("############# 4 :")
    import org.apache.spark.sql.expressions.Window
    dfA.withColumn(
      "Collection",
      collect_list(struct("Score", "ItemKey", "ItemName"))
        .over(Window.partitionBy("ItemKey"))
    )
      .select("UserKey", "Collection")
      .show(20, false)
  }
}

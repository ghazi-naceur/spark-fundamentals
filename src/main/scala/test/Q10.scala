package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dense_rank, explode, max, first}

object Q10 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Temp Data").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val spark = SparkSession.builder().getOrCreate()
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val peopleDF = Seq(
      ("Ali", 0, Seq(100)),
      ("Barbara", 1, Seq(300, 250, 100)),
      ("Cesar", 1, Seq(350, 100)),
      ("Dongmei", 1, Seq(400, 100)),
      ("Eli", 2, Seq(250)),
      ("Florita", 2, Seq(500, 300, 100)),
      ("Gatimu", 3, Seq(300, 100))
    ).toDF("name", "department", "score")

    peopleDF
      .withColumn("score", explode(col("score"))).show()

    peopleDF
      .withColumn("score", explode(col("score"))).groupBy("department").max("score").show()

    peopleDF
      .withColumn("score", explode(col("score"))).groupBy("department", "name").max("score").show()

    println("Solution 1 :")
    peopleDF
      .withColumn("score", explode(col("score")))
      .groupBy("department")
      .max("score")
      .withColumnRenamed("max(score)", "highest")
      .orderBy("department")
      .show()

    println("Solution 2 :")
//      peopleDF
//        .withColumn("score", explode(col("score")))
//        .orderBy("department", "score")
//        .select(col("name"), col("department"), first(col("score")).as("highest"))
//        .show()
    // => Error : no column having the name 'name'
    peopleDF
      .withColumn("score", explode(col("score")))
      .orderBy("department", "score").show()

    println("Solution 3 :")
    val maxByDept = peopleDF
      .withColumn("score", explode(col("score")))
      .groupBy("department")
      .max("score")
      .withColumnRenamed("max(score)", "highest")

    maxByDept
      .join(peopleDF, "department")
      .select("department", "name", "highest")
      .orderBy("department")
      .dropDuplicates("department")
      .show()

    maxByDept.show()
    peopleDF.show()
    println("Just joining ...")
    maxByDept
      .join(peopleDF, "department").show()

    println("Solution 4 :")
    import org.apache.spark.sql.expressions.Window
    // DESC Order scores by department
    val windowSpec2 = Window.partitionBy("department").orderBy(col("score").desc)
    peopleDF.withColumn("score", explode(col("score")))
      .select(col("department"), col("name"),
        dense_rank().over(windowSpec2).alias("rank"),
        max(col("score")).over(windowSpec2).alias("highest"))
      .where(col("rank") === 1)
      .drop(col("rank"))
      .orderBy("department")
      .show()
    val windowSpec = Window.partitionBy("department").orderBy(col("score").desc)
    peopleDF
      .withColumn("score", explode(col("score")))
      .select(
        col("department"),
        col("name"),
        dense_rank().over(windowSpec).alias("rank"),
        max(col("score")).over(windowSpec).alias("highest")
      )
      .where(col("rank") === 1)
      .drop("rank")
      .orderBy("department")
      .show()

    println("window 1:")
    peopleDF
      .withColumn("score", explode(col("score")))
      .select(
        col("department"),
        col("name")
//        dense_rank().over(windowSpec).alias("rank"),
//        max(col("score")).over(windowSpec).alias("highest")
      )
//      .where(col("rank") === 1)
//      .drop("rank")
      .orderBy("department")
      .show()

    println("window 2:")
    peopleDF
      .withColumn("score", explode(col("score")))
      .select(
        col("department"),
        col("name"),
        dense_rank().over(windowSpec).alias("rank")
//        max(col("score")).over(windowSpec).alias("highest")
      )
//      .where(col("rank") === 1)
//      .drop("rank")
      .orderBy("department")
      .show()

    println("window 3:")
    peopleDF
      .withColumn("score", explode(col("score")))
      .select(
        col("department"),
        col("name"),
        //        dense_rank().over(windowSpec).alias("rank"),
        max(col("score")).over(windowSpec).alias("highest")
      )
//      .where(col("rank") === 1)
//      .drop("rank")
      .orderBy("department")
      .show()

    println("window 4:")
    peopleDF
      .withColumn("score", explode(col("score")))
      .select(
        col("department"),
        col("name"),
        dense_rank().over(windowSpec).alias("rank"),
        max(col("score")).over(windowSpec).alias("highest")
      )
//      .where(col("rank") === 1)
//      .drop("rank")
      .orderBy("department")
      .show()
  }
}

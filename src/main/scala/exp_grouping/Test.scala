package exp_grouping
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, RelationalGroupedDataset, SparkSession}

object Test extends App {

  // Question 6 :

  val conf: SparkConf = new SparkConf().setAppName("Temp Data").setMaster("local[2]").set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")


  val spark = SparkSession.builder().getOrCreate()
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  val a: Array[Int] = Array(1002, 3001, 4002, 2003, 2002, 3004, 1003, 4006)

  val b: DataFrame = spark
    .createDataset(a)
    .withColumn("x", col("value") % 1000)
  b.show()

  val c = b
    .groupBy(col("x"))
    .agg(count("x"), sum("value"))
    c.show()
  val d = c.drop("x")
    .toDF("count", "total")
    .orderBy(col("count").desc, col("total"))
//    .limit(1)
    .show()

  // Question 8 :

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

  println("############# 3 :")
  dfA.groupBy("UserKey", "ItemKey", "ItemName")
    .agg(sort_array(collect_list(struct("Score", "ItemKey", "ItemName")), false))
    .drop("ItemKey", "ItemName")
    .toDF("UserKey", "Collection")
    .show(20, false)

  println("############# 4 :")
  import org.apache.spark.sql.expressions.Window
  dfA.withColumn(
    "Collection",
    collect_list(struct("Score", "ItemKey", "ItemName"))
      .over(Window.partitionBy("ItemKey"))
  )
    .select("UserKey", "Collection")
    .show(20, false)

  // Question 10 :

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

  println("Solution 1 :")
  peopleDF
    .withColumn("score", explode(col("score")))
    .groupBy("department")
    .max("score")
    .withColumnRenamed("max(score)", "highest")
    .orderBy("department")
    .show()

  println("Solution 2 :")
//  peopleDF
//    .withColumn("score", explode(col("score")))
//    .orderBy("department", "score")
//    .select(col("name"), col("department"), first(col("score")).as("highest"))
//    .show()

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

  println("Solution 4 :")
  import org.apache.spark.sql.expressions.Window
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
}

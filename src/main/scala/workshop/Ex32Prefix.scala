package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

object Ex32Prefix {

  def main(args: Array[String]): Unit = {

    /**
     @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Finding-Most-Common-Non-null-Prefix-Occurences-per-Group.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val input = Seq(
      (1, "Mr"),
      (1, "Mme"),
      (1, "Mr"),
      (1, null),
      (1, null),
      (1, null),
      (2, "Mr"),
      (3, null)).toDF("UNIQUE_GUEST_ID", "PREFIX")

    input.show()

    input.groupBy("UNIQUE_GUEST_ID")
      .agg(collect_list(col("PREFIX")))
      .orderBy("UNIQUE_GUEST_ID")
      .show()

    def findMostFrequentWord[A](list: List[A]): A = list.groupBy(identity).maxBy(_._2.size)._1

    val findMostFrequentWordUdf = spark.udf.register("find_most_frequent_word_udf", findMostFrequentWord[String](_: List[String]): String)

//    strings.select(my_upper_udf(col("strings"))).show()
//
//    strings.selectExpr("my_upper_udf(strings)").show()

    input.groupBy("UNIQUE_GUEST_ID")
      .agg(collect_list(col("PREFIX")).as("list"))
//      .withColumn("PREFIX", first(col("list")))
//      .withColumn("PREFIX", findMostFrequentWordUdf(col("list")))
      .map(row => row.getAs[scala.collection.mutable.WrappedArray[(String, Int)]]("list").groupBy(identity).map(kv => {
        if (kv == null)
          null
        else
          kv._1
      }))
//      .orderBy("UNIQUE_GUEST_ID")
      .show()

    val value = List(1,1,1,2,3,5,6).groupBy(identity).maxBy(_._2.size)._1
    println(value)
  }
}

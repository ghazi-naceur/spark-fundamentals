package workshop

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex4 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val nums = Seq(Seq(1,2,3)).toDF("nums")
    nums.show(false)

    println("Solution 1 :")
    val frame1 = nums.withColumn("num", explode(col("nums")))
    frame1.show(false)

    println("Solution 2 :")
    val frame2 = nums
      .as[Seq[Int]]
      .flatMap(identity)
      .toDF("num")
        .crossJoin(nums)
        .select(List("nums", "num").map(fd => col(fd)):_*)

    frame2.show(false)
  }
}

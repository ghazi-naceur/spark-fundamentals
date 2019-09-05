package workshop

import org.apache.spark.sql.SparkSession

object Ex4DSFlatMap {

  def main(args: Array[String]): Unit = {

    /**
     * @link https://github.com/jaceklaskowski/spark-workshop/blob/gh-pages/exercises/spark-sql-exercise-Using-Dataset-flatMap-Operator.md
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val input = Seq(
      Seq(1, 2, 3)
    )

    val inputDF = input
      .toDF("nums")

    inputDF.show()

    val numsDS = inputDF.as[Seq[Int]]
    numsDS
      .flatMap(identity)
      .toDF("num")
      .crossJoin(inputDF)
      .show(20, false)
  }
}
package workshop

import org.apache.spark.sql.SparkSession

object EX12UDF {

  def main(args: Array[String]): Unit = {

    /**
      @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Using-UDFs.html
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val strings = Seq(Seq("a", "b", "c")).toDF("strings")

    def my_upper(strs: Seq[String]): Seq[String] = strs.map(s => s.toUpperCase)

    val my_upper_udf = spark.udf.register("my_upper_udf", my_upper(_:Seq[String]):Seq[String])

    strings.select(my_upper_udf(col("strings"))).show()

    strings.selectExpr("my_upper_udf(strings)").show()

  }
}

package test

import org.apache.spark.sql.SparkSession

object GroupByPartitionByExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._

    val rawData = Seq(
      (1, "arun", "prasanth", 40),
      (2, "ann", "antony", 45),
      (3, "sruthy", "abc", 41),
      (6, "new", "abc", 47),
      (1, "arun", "prasanth", 45),
      (1, "arun", "prasanth", 49),
      (2, "ann", "antony", 49)
    )

    val tableA = spark
      .createDataFrame(rawData)
      .toDF("id", "firstname", "lastname", "Mark")

    tableA.show()
    tableA.createTempView("users")

    spark.sql("select SUM(Mark)marksum,firstname from users group by id,firstName").show()

    spark.sql("SELECT SUM(Mark) OVER (PARTITION BY id) AS marksum, firstname FROM users").show()
//

  }
}

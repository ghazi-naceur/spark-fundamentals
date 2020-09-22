package test

import org.apache.spark.sql.SparkSession

object RepartitionExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._

    val dfA = Seq(
      (10,"A","DE"),
      (10,"A","IND"),
      (20,"B","DE"),
      (20,"B","IND")
    )

    val dfB = Seq(
      (10,"A","IND"),
      (20,"B","IND"),
      (10,"A","DE"),
      (20,"B","DE")
    )

    val tableA = spark
      .createDataFrame(dfA)
      .toDF("ID", "name", "ADDR_A")
    tableA.createTempView("tabA")

    val tableB = spark
      .createDataFrame(dfB)
      .toDF("ID", "name", "ADDR_B")
    tableA.createTempView("tabB")

    val dfRes = tableA.crossJoin(tableB.select("ADDR_B")).select("ID", "name", "ADDR_B")

    tableA.show()
    tableB.show()
    dfRes.show()
  }
}

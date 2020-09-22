package test

import org.apache.spark.sql.{SaveMode, SparkSession}

object SelectingParquetFile {

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("CSV-Parquet").master("local[*]").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    ss.read.parquet("C:\\tmp\\datasets\\rejected\\sales\\customers").show(30, false)

    ss.read.parquet("C:\\tmp\\datasets\\accepted\\sales\\customers").show(30, false)
  }
}

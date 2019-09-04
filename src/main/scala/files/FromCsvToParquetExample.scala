package files

import org.apache.spark.sql.{SaveMode, SparkSession}

object FromCsvToParquetExample {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().appName("CSV-Parquet").master("local[*]").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    ss.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

    val loadedCsv = ss.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ",")
      .csv("src/main/resources/from-csv-to-parquet/stats.csv")

    loadedCsv.write.mode(SaveMode.Overwrite).parquet("src/main/resources/from-csv-to-parquet/gen-par")
    Thread.sleep(5000)

    val generatedParquet = ss.read.parquet("src/main/resources/from-csv-to-parquet/gen-par")
    generatedParquet.show()
  }
}

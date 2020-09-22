package official.doc.examples.sql.c2_data_sources

import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class Record(key: Int, value: String)

object HiveTables {

  def main(args: Array[String]): Unit = {

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'src/main/resources/official.doc/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()

    // Create a Hive managed Parquet table, with HQL syntax instead of the Spark SQL native syntax
    // `USING hive`
    sql("CREATE TABLE IF NOT EXISTS hive_records(key int, value string) STORED AS PARQUET")
    // Save DataFrame to the Hive managed table
    val df = spark.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
    // After insertion, the Hive managed table has data now
    sql("SELECT * FROM hive_records").show()

    // Prepare a Parquet data directory
    val dataDir = "C:/tmp/parquet_data"
    spark.range(10).write.mode(SaveMode.Overwrite).parquet(dataDir)
    // Create a Hive external Parquet table
    sql(s"CREATE EXTERNAL TABLE IF NOT EXISTS hive_ints(key int) STORED AS PARQUET LOCATION '$dataDir'")
    // The Hive external table should already have data
    sql("SELECT * FROM hive_ints").show()


    // Turn on flag for Hive Dynamic Partitioning
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // Create a Hive partitioned table using DataFrame API
    df.write.mode(SaveMode.Overwrite).partitionBy("key").format("hive").saveAsTable("hive_part_tbl")
    // Partitioned column `key` will be moved to the end of the schema.
    sql("SELECT * FROM hive_part_tbl").show()

//    import org.apache.spark.sql.functions.broadcast
//    broadcast(spark.table("src")).join(spark.table("records"), "key").show()

    spark.close()
  }
}

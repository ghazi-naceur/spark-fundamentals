package official.doc.examples.streaming.structured.streaming

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.types.StructType

object Sinks {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

//    val userSchema = new StructType().add("device", "string").add("deviceType", "string")
//      .add("signal", "double").add("time", "timestamp")

    val path = "src/main/resources/official.doc/iot.json"
    val df = spark.readStream.json(path)

    // File sink :
  /**  df.writeStream
      .format("parquet")        // can be "orc", "json", "csv", etc.
      .option("path", "iot/parquet/iot-parquet")
      .start()
*/
    // Kafka sink :
  /**  df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("topic", "updates")
      .start()
    */
    // Foreach sink :
  /** df.writeStream
      .foreach((writer: ForeachWriter[Row]) => writer.process())
    .start()
   */

    // Console sink :
  /**  df.writeStream
      .format("console")
      .start()
  */
    // Memory sink :
  /**  df.writeStream
      .format("memory")
      .queryName("tableName")
      .start()
  */

    // ========== DF with no aggregations ==========
    val noAggDF = df.select("device").where("signal > 10")

    // Print new data to console
 /**   noAggDF
      .writeStream
      .format("console")
      .start()
*/
    // Write new data to Parquet files
 /**   noAggDF
      .writeStream
      .format("parquet")
      .option("checkpointLocation", "path/to/checkpoint/dir")
      .option("path", "path/to/destination/dir")
      .start()
*/
    // ========== DF with aggregation ==========
    val aggDF = df.groupBy("device").count()

    // Print updated aggregations to console
/**    aggDF
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()
*/
    // Have all the aggregates in an in-memory table
/**    aggDF
      .writeStream
      .queryName("aggregates")    // this query name will be the table name
      .outputMode("complete")
      .format("memory")
      .start()

    spark.sql("select * from aggregates").show()   // interactively query in-memory table
 */
  }
}

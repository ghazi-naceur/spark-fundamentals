package official.doc.examples.streaming.structured.streaming

import org.apache.spark.sql.SparkSession

object StructuredStreamingJoins {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
    .builder()
    .appName("Spark Structured Streaming Example")
    .config("spark.master", "local")
    .getOrCreate()

    // 1- Stream-Static Join :

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val streamingDf = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    val path = "src/main/resources/official.doc/people.json"
    val staticDf = spark.read.json(path)
/**
    streamingDf.join(staticDf)          // inner equi-join with a static DF
//    streamingDf.join(staticDf, "type", "right_join")  // right outer join with a static DF

    val query = streamingDf.writeStream
      .outputMode("update")
      .format("console")
      .start()
//    Thread.sleep(20000)
//    query.awaitTermination()
    streamingDf.show()
*/

  // Stream-Stream Join :
/**    import org.apache.spark.sql.functions._
    val impressions = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    val clicks = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9998)
      .load()

    // Apply watermarks on event-time columns
    val impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours")
    val clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours")

    // Join with event-time constraints
    impressionsWithWatermark.join(
      clicksWithWatermark,
      expr("""
    clickAdId = impressionAdId AND
    clickTime >= impressionTime AND
    clickTime <= impressionTime + interval 1 hour
    """)
    )

    impressionsWithWatermark.join(
      clicksWithWatermark,
      expr("""
    clickAdId = impressionAdId AND
    clickTime >= impressionTime AND
    clickTime <= impressionTime + interval 1 hour
    """),
      joinType = "leftOuter"      // can be "inner", "leftOuter", "rightOuter"
    )
 */
    // Deduplication
/**
    // Without watermark using guid column
    streamingDf.dropDuplicates("guid")

    // With watermark using guid and eventTime columns
    streamingDf
      .withWatermark("eventTime", "10 seconds")
      .dropDuplicates("guid", "eventTime")
*/
  }
}

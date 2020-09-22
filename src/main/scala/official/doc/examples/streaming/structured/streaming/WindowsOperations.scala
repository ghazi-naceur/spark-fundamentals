package official.doc.examples.streaming.structured.streaming

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.window

object WindowsOperations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    val path = "src/main/resources/official.doc/window.json"

    val words = spark.read.json(path) // streaming DataFrame of schema { timestamp: Timestamp, word: String }
    words.show()

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
      window($"timestamp", "10 minutes", "5 minutes"),
      $"word"
    ).count()
    windowedCounts.show()

    // Group the data by window and word and compute the count of each group
    // The threshold of how late is the data allowed to be = 10 min
    val windowedCounts2 = words
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "10 minutes", "5 minutes"),
        $"word")
      .count()

    windowedCounts2.show()

  }
}

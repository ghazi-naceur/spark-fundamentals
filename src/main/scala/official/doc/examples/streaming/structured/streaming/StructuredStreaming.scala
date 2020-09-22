package official.doc.examples.streaming.structured.streaming

import org.apache.spark.sql.SparkSession

object StructuredStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Structured Streaming Example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    // We have converted the DataFrame to a Dataset of String using .as[String],
    // so that we can apply the flatMap operation to split each line into multiple words.
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    // We have defined the wordCounts DataFrame by grouping by the unique values in the Dataset
    // and counting them. Note that this is a streaming DataFrame which represents the running word counts of the stream.
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

    // To see the jobs' execution, you can use Spark UI

    /**
      We open a CLI to start the NC server :
        > nc -l -p 9999
      After that, we start typing in nc's console and monitoring the intellij's console

     1- Open NC CLI
     2- Launch this example
     3- Starting typing on NC's CLI
     4- Kill the NC's CLI (Ctrl+c)
     5- Monitor Intellij's console.
      */
  }
}

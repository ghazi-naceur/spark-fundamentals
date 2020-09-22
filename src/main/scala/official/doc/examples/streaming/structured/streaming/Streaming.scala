package official.doc.examples.streaming.structured.streaming

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.types.StructType

object Streaming {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Structured Streaming Example")
      .config("spark.master", "local")
      .getOrCreate()

    // Read text from socket
    val socketDF = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    println(socketDF.isStreaming)    // Returns True for DataFrames that have streaming sources

    println(socketDF.printSchema)



    // Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema)      // Specify schema of the csv files
      .csv("src/main/resources/official.doc/csv")    // Equivalent to format("csv").load("/path/to/directory")

//      .csv("src/main/resources/official.doc/people.csv")    // Equivalent to format("csv").load("/path/to/directory")
//    Error : org.apache.spark.sql.streaming.StreamingQueryException: Option 'basePath' must be a directory
//    You should provide a folder, not a file


//    csvDF.show() // This command won't work because readStream doesn't support show method. You need to write down
//    with the help of a sink and then display it.
/**
    TL;DR :
      Solving AnalysisException: Queries with streaming sources must be executed with writeStream.start()
    Since the problem is our batch-oriented code, nothing more simple to fix it than converting the code to streaming-oriented.
      In structured streaming Spark programmers introduced the concepts of sources and sinks. The second ones define how
    the data is consumed. There are several basic sinks, as foreach (data read in foreach loop), console (data printed to the console)
    or file (data persisted to files).

      The sinks are created directly from read stream by calling writeStream() method on loaded DataFrame. It will create an instance
    of DataStreamWriter class that can be used to consume streamed data just after calling its start() method.
*/
    // 1st Sink :
    /**
    val query = csvDF.writeStream.format("console").start()
    query.awaitTermination()
    */

    // 2nd Sink :
    /**
    val queryfw = csvDF.writeStream
      .foreach(new ForeachWriter[Row] {
        override def process(row: Row): Unit = {
          println(s"Processing ${row}")
        }

        override def close(errorOrNull: Throwable): Unit = {}

        override def open(partitionId: Long, version: Long): Boolean = {
          true
        }
      })
      .start()
    queryfw.awaitTermination()
     */
  }
}

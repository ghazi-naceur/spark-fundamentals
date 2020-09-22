package official.doc.examples.streaming.streaming.pgr

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

// It is nearly the same example as :
//    C:\workspace\spark-fundamentals\src\main\scala\official\doc\examples\streaming\structured\streaming\StructuredStreaming.scala
object StreamingContext {

  def main(args: Array[String]): Unit = {

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent a starvation scenario.
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//    val ssc = new StreamingContext(conf, Seconds(1))
    val ssc = new StreamingContext(conf, Seconds(3))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

    /**
    We open a CLI to start the NC server :
        > nc -l -p 9999
      After that, we start typing in nc's console and monitoring the intellij's console
      */
  }
}

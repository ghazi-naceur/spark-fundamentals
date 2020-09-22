package official.doc.examples.getting_started

import org.apache.spark.sql.{Dataset, SparkSession}

object Shell {

  def main(args: Array[String]): Unit = {

    // Run ./bin/spark-shell
    val ss = SparkSession.builder().master("local[*]").getOrCreate()
    import ss.implicits._

    val textFile = ss.read.textFile("src/main/resources/file.xml")
    textFile.count // nb of items
    textFile.first // first element in the list
    val linesWithSpark = textFile.filter(line => line.contains("page"))
    textFile.filter(line => line.contains("page")).count()
    val lineWithBiggestNumberOfWords = textFile.map(line => line.split(" ").length).reduce((a, b) => if (a > b) a else b) // return longest line
    println(lineWithBiggestNumberOfWords)
    import java.lang.Math
    val lineWithBiggestNumberOfWords2 = textFile.map(line => line.split(" ").length).reduce((a, b) => Math.max(a, b))
    // return longest line as well (It is much more easier)
    println(lineWithBiggestNumberOfWords2)

    val lineWithBiggestNumberOfWords3 = textFile.map(line => line.split(" ").length).reduce(_ max _)
    println(lineWithBiggestNumberOfWords3)

    val longestWord = textFile.flatMap(line => line.split(" ")).reduce((ws1, ws2) => if (ws1.length > ws2.length) ws1 else ws2)
    println(longestWord)

//
//    Implementation for MapReduce :
    val wordCounts = textFile.flatMap(line => line.split(" ")).groupByKey(identity).count() // Obtaining nb of occurrences of each word
    wordCounts.show()

    val linesWithSpark2 = textFile.map(line => line.split(" "))
    linesWithSpark2.cache()

    val logFile = "src/main/resources/file.xml" // Should be some file on your system
    val logData: Dataset[String] = ss.read.textFile(logFile).cache()

    val linesWithA = logData.filter(line => line.contains("a"))
    linesWithA.show()
    val numAs = logData.filter(line => line.contains("a")).count()

    val linesWithB = logData.filter(line => line.contains("b"))
    linesWithB.show()
    val numBs = logData.filter(line => line.contains("b")).count()

    println(s"Lines with a: $numAs, Lines with b: $numBs")
//    ss.stop()

//spark-submit --class "Shell" --master local[4] C:/workspace/spark-fundamentals/target/scala-2.12/spark-fundamentals_2.12-0.1.jar

  }
}

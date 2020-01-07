package workshop

import org.apache.spark.sql.{Dataset, SparkSession}

object Ex3FindIds {

  def main(args: Array[String]): Unit = {

    /**
     @link http://blog.jaceklaskowski.pl/spark-workshop/exercises/spark-sql-exercise-Finding-Ids-of-Rows-with-Word-in-Array-Column.html

     input :
      +---+------------------+-----+
      | id|             words| word|
      +---+------------------+-----+
      |  1|     one,two,three|  one|
      |  2|     four,one,five|  six|
      |  3|seven,nine,one,two|eight|
      |  4|    two,three,five| five|
      |  5|      six,five,one|seven|
      +---+------------------+-----+

     output :
      +-----+------------+
      |split|ids         |
      +-----+------------+
      |two  |[1, 3, 4]   |
      |seven|[3]         |
      |four |[2]         |
      |one  |[1, 2, 3, 5]|
      |six  |[5]         |
      |nine |[3]         |
      |three|[1, 4]      |
      |five |[2, 4, 5]   |
      +-----+------------+
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val input = Seq(
      (1, "one,two,three", "one"),
      (2, "four,one,five", "six"),
      (3, "seven,nine,one,two", "eight"),
      (4, "two,three,five", "five"),
      (5, "six,five,one", "seven")
    )

    val inputDF = spark
      .createDataFrame(input)
      .toDF("id", "words", "word")

    inputDF.show()

    import org.apache.spark.sql.functions._

    inputDF.map(row => row.getString(1).split(",")).show(20, false)
    inputDF.select(split(col("words"), ",")).show(20, false) // The same as the previous
    println("Step 1 :")
    inputDF.selectExpr("id", "words", "split(words, ',') as split", "word").show(20, false)

    println("Step 2 :")
    inputDF.selectExpr("id", "words", "split(words, ',') as split", "word")
      .drop("words") // I can omit it from the beginning (selectExpr), I've decided to put it anyway
      .show(20, false)

    println("Step 3 :")
    inputDF.selectExpr("id", "words", "split(words, ',') as split", "word")
      .drop("words")
      .withColumn("split", explode(col("split")))
      .show(20, false)

    println("Step 4 :")
    inputDF.selectExpr("id", "split(words, ',') as split", "word")
      .withColumn("split", explode(col("split")))
      .show(20, false)

    println("Step 5 :")
    inputDF.selectExpr("id", "split(words, ',') as split", "word")
      .withColumn("split", explode(col("split")))
      .groupBy("split").agg(collect_list("id").as("ids"))
      .show(20, false)
  }
}
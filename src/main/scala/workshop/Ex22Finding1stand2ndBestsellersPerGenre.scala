package workshop

import org.apache.spark.sql.SparkSession

object Ex22Finding1stand2ndBestsellersPerGenre {

  def main(args: Array[String]): Unit = {

    /**
     * @link https://github.com/jaceklaskowski/spark-workshop/blob/gh-pages/exercises/spark-sql-exercise-Finding-1st-and-2nd-Bestsellers-Per-Genre.md
     */

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import java.nio.charset.StandardCharsets
    import java.nio.file.{Files, Paths, StandardOpenOption}

    import spark.implicits._
    def delete(filePath: String) = Files.deleteIfExists(Paths.get(filePath))

    def write(filePath: String, contents: String) = {
      Files.write(Paths.get(filePath), contents.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE)
    }

    val books =
      """
      id,title,genre,quantity
      1,Hunter Fields,romance,15
      2,Leonard Lewis,thriller,81
      3,Jason Dawson,thriller,90
      4,Andre Grant,thriller,25
      5,Earl Walton,romance,40
      6,Alan Hanson,romance,24
      7,Clyde Matthews,thriller,31
      8,Josephine Leonard,thriller,1
      9,Owen Boone,sci-fi,27
      10,Max McBride,romance,75
      """.trim.stripMargin
    delete("target/books.csv")
    write("target/books.csv", books)

    val inputDF = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("target/books.csv")

    inputDF.show()

    import org.apache.spark.sql.expressions._
    import org.apache.spark.sql.functions._

    val byGenreWidow = Window.partitionBy("genre").orderBy(desc("quantity"))
    inputDF.select(
      'id.cast("integer"),
      'title,
      'genre,
      'quantity,
      rank().over(byGenreWidow).as("rank")
    ).where("rank <= 2")
      .show()

  }
}
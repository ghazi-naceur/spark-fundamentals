package udemy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.functions._

object PopularMovieFinder {

  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    import spark.sqlContext.implicits._

    val movieSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    val movieDetailsSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)
      .add("releaseDate", StringType, nullable = true)
      .add("imdbLink", StringType, nullable = true)

    val movies = spark.read
      .option("sep", "\t")
      .schema(movieSchema)
      .csv("src/main/resources/udemy/u.data")

    movies.show(100, false)

    val grouped = movies.groupBy("movieID").count().orderBy(desc("count"))
    grouped.show(false)


    val details = spark.sparkContext.textFile("src/main/resources/udemy/u.item")
    val detailedMovies = details.map(line => {
      val columns = line.split('|').toList
      Row(columns.head.toInt, columns(1), columns(2), columns(4))
    })
    val detailedMoviesDF = spark.createDataFrame(detailedMovies, movieDetailsSchema)
    detailedMoviesDF.show(false)

    val joined = grouped.join(detailedMoviesDF, "movieID").orderBy(desc("count"))
    joined.show(false)
  }
}

case class MovieDetails(movieID: Int, name: String, releaseDate: String, imdbLink: String)
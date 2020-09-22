package official.doc.examples.sql.c2_data_sources

import org.apache.spark.sql.SaveMode

object GenericLoadSave {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .getOrCreate()
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

    /**
    val usersDF = spark.read.load("src/main/resources/official.doc/users.parquet")
    usersDF.select("name", "favorite_color").write.mode(SaveMode.Overwrite).save("namesAndFavColors.parquet")
*/
    /****    Manually Specifying Options   ****/

    val peopleDF = spark.read.format("json").load("src/main/resources/official.doc/people.json")
    peopleDF.select("name", "age").write.mode(SaveMode.Overwrite).format("parquet").save("namesAndAges.parquet")
    peopleDF.show()

    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("src/main/resources/official.doc/people.csv")

    peopleDFCsv.show()

    peopleDF.write.mode(SaveMode.Overwrite).format("orc")
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .save("users_with_options.orc")


    val sqlDF = spark.sql("SELECT * FROM parquet.`src/main/resources/official.doc/users.parquet`")
    sqlDF.show()


    /*****    Bucketing, Sorting and Partitioning   *****/
/**
    val peopleDF2 = spark.read.format("json").load("src/main/resources/official.doc/people.json")
    peopleDF2.select("name", "age").write.mode(SaveMode.Overwrite).format("parquet").save("namesAndAges.parquet")
    peopleDF2.write.mode(SaveMode.Overwrite).bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    peopleDF2.show()

    spark.sql("SELECT * FROM people_bucketed").show()
    // => For file-based data source, it is also possible to bucket and sort or partition the output.
    //      Bucketing and sorting are applicable only to persistent tables
*/

    val usersDF = spark.read.load("src/main/resources/official.doc/users.parquet")
    usersDF.select("name", "favorite_color").write.mode(SaveMode.Overwrite).save("namesAndFavColors.parquet")
    usersDF.write.mode(SaveMode.Overwrite).partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

    usersDF
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("users_partitioned_bucketed")

    usersDF.show()
  }
}

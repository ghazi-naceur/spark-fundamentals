package official.doc.examples.sql.c2_data_sources

import org.apache.spark.sql.SaveMode

object ParquetFiles {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .getOrCreate()

    /******     Loading Data Programmatically    *******/

    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._

    val peopleDF = spark.read.json("src/main/resources/official.doc/people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF.write.mode(SaveMode.Overwrite).parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
    namesDF.map(attributes => "Name: " + attributes.getAs("name")).show()

    /*******   Schema Merging   *******/

    // This is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.mode(SaveMode.Overwrite).parquet("data/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.mode(SaveMode.Overwrite).parquet("data/test_table/key=2")
    spark.read.parquet("data/test_table/key=1").show()
    spark.read.parquet("data/test_table/key=2").show()
    spark.read.parquet("data/test_table").show() //it shows everything for key=1 , and set null to key=2 values

    // Read the partitioned table
    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()
    // => The final schema consists of all 3 columns in the Parquet files together
    //    with the partitioning column appeared in the partition directory paths
    //    root

  }
}

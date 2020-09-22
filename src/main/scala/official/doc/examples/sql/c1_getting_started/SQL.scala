package official.doc.examples.sql.c1_getting_started

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
// This class should be outside the SQL object (out of the scope of SQL object), otherwise ".toDS" function won't work !
case class Person(name: String, age: Long)

object SQL {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .getOrCreate()
    //      .appName("Spark SQL basic example")
    //      .config("spark.executor.memory", "1g")

    /*******  Creating DataFrames ********/

    import spark.implicits._
    // For implicit conversions like converting RDDs to DataFrames

    val df = spark.read.json("src/main/resources/official.doc/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()

    /*********  Untyped Dataset Operations (aka DataFrame Operations)  **********/
    // Print the schema in a tree format
       df.printSchema()

    // Select only the "name" column
    df.select("name").show()

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()

    // Select people older than 21
    df.filter($"age" > 21).show()

    // Count people by age
    df.groupBy("age").count().show()

    /*****  Running SQL Queries Programmatically  ********/

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    /******  Global Temporary View  ******/
    /**
    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    // Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    */
    /*******   Creating Datasets  ******/
    /**
    import spark.implicits._
    // Encoders are created for case classes
    val caseClassDS: Dataset[Person] = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    */
    /**
    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    val res = primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
    println(res.toList)
    */
    /**
   // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "src/main/resources/official.doc/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
    */
    /*********  Inferring the Schema Using Reflection  **********/
    /**
    // For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF2 = spark.sparkContext
      .textFile("src/main/resources/official.doc/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF2.createOrReplaceTempView("people")
    peopleDF2.show()

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show() // index 0 because in the Select Statement, we put : "SELECT name, age ..."
                                                              //    name after that age .. name with index 0 and age with index 1

    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    implicit val mapEncoder: Encoder[Map[String, Any]] = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    val persons = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    println(persons.toList)
    // Array(Map("name" -> "Justin", "age" -> 19))
    */

    /********   Programmatically Specifying the Schema  *******/

    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("src/main/resources/official.doc/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields: Array[StructField] = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    println(schema.toList)

    // Convert records of the RDD (people) to Rows
    val rowRDD: RDD[Row] = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF: DataFrame = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")
//    peopleDF.show()

    // SQL can be run over a temporary view created using DataFrames
    val names: DataFrame = spark.sql("SELECT name FROM people")

    names.show()
    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
//    results.map(attributes => "Name: " + attributes(0)).show()
  }
}

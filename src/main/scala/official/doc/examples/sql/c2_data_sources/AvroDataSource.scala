package official.doc.examples.sql.c2_data_sources

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.avro._

object AvroDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val usersDF = spark.read.format("avro").load("src/main/resources/official.doc/users.avro")
    usersDF.select("name", "favorite_color").write
      .mode(SaveMode.Overwrite).format("avro").save("namesAndFavColors.avro")
    usersDF.show()

    // `from_avro` requires Avro schema in JSON string format.
    val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("./src/main/resources/official.doc/user.avsc")))
    println(jsonFormatSchema)

    /**
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()

    // 1. Decode the Avro data into a struct;
    // 2. Filter by column `favorite_color`;
    // 3. Encode the column `name` in Avro format.
    val output = df
      .select(from_avro('value, jsonFormatSchema) as 'user)
      .where("user.favorite_color == \"red\"")
      .select(to_avro($"user.name") as 'value)

    val query = output
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("topic", "topic2")
      .start()
     */
  }
}

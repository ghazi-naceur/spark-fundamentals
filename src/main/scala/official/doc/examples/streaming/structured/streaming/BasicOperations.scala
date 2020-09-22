package official.doc.examples.streaming.structured.streaming

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class DeviceData(device: String, deviceType: String, signal: Double, time: Timestamp)

object BasicOperations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    val path = "src/main/resources/official.doc/iot.json"
    val df = spark.read.json(path)
    // streaming DataFrame with IOT device data with schema { device: string, deviceType: string, signal: double, time: string }

    val ds: Dataset[DeviceData] = df.as[DeviceData]    // streaming Dataset with IOT device data

    // Select the devices which have signal more than 10
    df.select("device").where("signal > 10").show()      // using untyped APIs
    ds.filter(_.signal > 10).map(_.device).show()       // using typed APIs

    // Running count of the number of updates for each device type
    val countWithUntypedApi = df.groupBy("deviceType").count() // using untyped API
    countWithUntypedApi.show()

    val countWithTypeApi = ds.groupBy("deviceType").count() // using typed API
    countWithTypeApi.show()

    // Running average signal for each device type
    import org.apache.spark.sql.expressions.scalalang.typed
    ds.groupByKey(_.deviceType).agg(typed.avg(_.signal)).show()    // using typed API
    import org.apache.spark.sql.functions._
    df.groupBy("deviceType").agg(avg("signal")).show() // using untyped API

    df.createOrReplaceTempView("updates")
    spark.sql("select count(*) from updates").show()  // returns another streaming DF
    println(df.isStreaming)
  }
}

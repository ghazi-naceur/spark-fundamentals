package official.doc.examples.getting_started

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Parallelized Collection
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    // External dataset
    val lines: RDD[String] = sc.textFile("src\\main\\resources\\file.xml")

    val lineLengths: RDD[Int] = lines.map(line => line.length)
    lineLengths.persist()

    val totalLength: Int = lineLengths.reduce(_ + _)
    println(totalLength)

    val list: RDD[String] = lines.flatMap(str => str.split(" "))
    val res = list.collect().filter(str => str.length != 0).toList
    println(res)

    // Count occurrences :
    val result: RDD[(String, Int)] = lines.flatMap(str => str.trim.split(" ")).map(str => (str, 1)).reduceByKey(_ + _)
    println(result.collect().toList)

    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    println(broadcastVar.value.toList)

    val accum = sc.longAccumulator("My Accumulator")
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum.add(x))
    println(accum.value)

    val acc = sc.longAccumulator
    data.map { x => acc.add(x); x }
    println(acc.value) // 15
    // Here, acc is still 0 because no actions have caused the map operation to be computed.
  }
}

package exp_grouping

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Grouping extends App {

  val conf: SparkConf = new SparkConf().setAppName("Temp Data").setMaster("local[2]").set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  // Method 1 : groupByKey
  private val resGroupByKey = sc.textFile("src/main/resources/mock1.csv")
    .flatMap(line => line.split(","))
    .map(word => (word, 1))
    .groupByKey
    .mapValues(values => values.sum)
    .sortBy(-_._2)
    .collect
    .toList
  println(resGroupByKey.take(5))

  // Method 2 : reduceByKey
  private val resReduceByKey: List[(String, Int)] = sc.textFile("src/main/resources/mock1.csv")
    .flatMap(line => line.split(","))
    .map(word => (word, 1))
    .reduceByKey((x, y) => x + y) //.reduceByKey(_+_)
    .sortBy(-_._2)
    .collect().toList
  println(resReduceByKey.take(5))

  // Method 3 : aggregateByKey
  val kv: RDD[(String, Int)] = sc.textFile("src/main/resources/mock1.csv").flatMap(_.split(","))
    .map(word => (word, 1))
  val initialCount = 0
  val addToCounts = (n: Int, x: Int) => n + x
  val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

    val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
      .sortBy(- _._2)
      .collect().toList
    println(countByKey.take(5))
}

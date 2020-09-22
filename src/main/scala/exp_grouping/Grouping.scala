package exp_grouping

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Grouping extends App {

  val conf: SparkConf = new SparkConf().setAppName("Temp Data").setMaster("local[2]").set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  private val resGroupByKey = sc.textFile("src/main/resources/mock1.csv")
    .flatMap(line => line.split(","))
    .map(word => (word, 1))
    .groupByKey
    .mapValues(values => values.sum)
    .sortBy(-_._2)
    .collect
    .toList
  println(resGroupByKey.take(5))

  private val resReduceByKey: List[(String, Int)] = sc.textFile("src/main/resources/mock1.csv")
    .flatMap(line => line.split(","))
    .map(word => (word, 1))
    .reduceByKey((x, y) => x + y)
    .sortBy(-_._2)
    .collect().toList
  println(resReduceByKey.take(5))

  //  val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
  //  val data = sc.parallelize(keysWithValuesList)
  //Create key value pairs
  val kv: RDD[(Array[String], Int)] = sc.textFile("src/main/resources/mock1.csv").map(_.split(","))
    .map(word => (word, 1))
  val initialCount = 0
  val addToCounts = (n: Int, x: Int) => n + x
  val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2
  // TODO : The code bellow generated an error :
  //  org.apache.spark.SparkException: Cannot use map-side combining with array keys.
  //  => Simply the error that Scala doesn't support an Array as a Key, so You need to convert it to a List :
  //  => kv.map(kv => (kv._1.toList, kv._2))
    val countByKey: List[(List[String], Int)] = kv.map(kv => (kv._1.toList, kv._2)).aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
      .sortBy(- _._2)
      .collect().toList
    println(countByKey.take(5))

//  private val aggByKey: List[(Array[String], Int)] = kv.aggregateByKey(0)((k, v) => v+k, (v, k) => k+v).sortBy(- _._2).collect.toList
//  println(aggByKey.take(5))

  private val occNb = sc.textFile("src/main/resources/mock1.csv")
    .flatMap(line => line.split(","))
    .map(word => (word, 1))
    .reduceByKey(_+_)
    .sortBy(- _._2)
    .collect
    .toList
  println(occNb.take(5))
}

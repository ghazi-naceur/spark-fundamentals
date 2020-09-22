package exp_grouping

import org.apache.spark.{SparkConf, SparkContext}

object League extends App {

  val conf: SparkConf = new SparkConf().setAppName("Temp Data").setMaster("local[2]").set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  val premierRDD = sc.parallelize(List(
    ("Arsenal", "2014–2015", 75), ("Arsenal", "2015–2016", 71), ("Arsenal", "2016–2017", 75), ("Arsenal", "2017–2018", 63),
    ("Chelsea", "2014–2015", 87), ("Chelsea", "2015–2016", 50), ("Chelsea", "2016–2017", 93), ("Chelsea", "2017–2018", 70),
    ("Liverpool", "2014–2015", 62), ("Liverpool", "2015–2016", 60), ("Liverpool", "2016–2017", 76), ("Liverpool", "2017–2018", 75),
    ("M. City", "2014–2015", 79), ("M. City", "2015–2016", 66), ("M. City", "2016–2017", 78), ("M. City", "2017–2018", 100),
    ("M. United", "2014–2015", 70), ("M. United", "2015–2016", 66), ("M. United", "2016–2017", 69), ("M. United", "2017–2018", 81)
    ))

  private val res = premierRDD
    .map(cup => (cup._1, 1))
    .reduceByKey((x, y) => x + y)
    .collect()
    .toList
  println(res)

  private val res2 = premierRDD
    .map(cup => (cup._1, cup._3))
    .groupByKey()
    .map(cup => (cup._1, cup._2.max))
    .collect()
    .toList
  println("winnerWithScore 0")
  println(res2)

  private val winnerWithScore: List[(String, Int)] = premierRDD.map(t => (t._1, t._3))
    .reduceByKey((x, y) => if (x > y) x else y)
    .collect()
    .toList
  println("winnerWithScore 1")
  println(winnerWithScore)

  private val winnerWithScore2: List[(String, Int)] = premierRDD.map(t => (t._1, t._3))
    .reduceByKey((x, y) => x.max(y))
    .collect()
    .toList
  println("winnerWithScore 2")
  println(winnerWithScore2)

  private val winnerWithScore3: List[(String, Int)] = premierRDD.map(t => (t._1, t._3))
    .reduceByKey(_.max(_))
    .collect()
    .toList
  println("winnerWithScore 3")
  println(winnerWithScore3)

  private val winnerWithScore4: List[(String, Int)] = premierRDD.map(t => (t._1, t._3))
    .reduceByKey(_ max _)
    .collect()
    .toList
  println("winnerWithScore 4")
  println(winnerWithScore4)
}

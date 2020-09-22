package exp_grouping

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Score extends App {

  val conf: SparkConf = new SparkConf().setAppName("Temp Data").setMaster("local[2]").set("spark.executor.memory", "1g")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

//  type ScoreCollector = (Int, Double)
//  type PersonScores = (String, (Int, Double))

  val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))

  val wilmaAndFredScores = sc.parallelize(initialScores).cache()

  //The createScoreCombiner takes a double value and returns a tuple of (Int, Double)
  val createScoreCombiner: Double => (Int, Double) = (score: Double) => (1, score)

  //The scoreCombiner function takes a ScoreCollector which is a type alias for a tuple of (Int,Double).
  // We alias the values of the tuple to numberScores and totalScore (sacrificing a one-liner for readability).
  // We increment the number of scores by one and add the current score to the total scores received so far.
  val scoreCombiner: ((Int, Double), Double) => (Int, Double) = (collector: (Int, Double), score: Double) => {
    val (numberScores, totalScore) = collector
    (numberScores + 1, totalScore + score)
  }

  // The scoreMerger function takes two ScoreCollectors adds the total number of scores and the total scores together
  // returned in a new tuple.
  val scoreMerger: ((Int, Double), (Int, Double)) => (Int, Double) = (collector1: (Int, Double), collector2: (Int, Double)) => {
    val (numScores1, totalScore1) = collector1
    val (numScores2, totalScore2) = collector2
    (numScores1 + numScores2, totalScore1 + totalScore2)
  }
  //We then call the combineByKey function passing our previously defined functions.
  val scores: RDD[(String, (Int, Double))] = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)

  //We take the resulting RDD, scores, and call the collectAsMap function to get our results in the form of
  // (name,(numberScores,totalScore)).
  val averagingFunction: ((String, (Int, Double))) => (String, Double) = (personScore: (String, (Int, Double))) => {
    val (name, (numberScores, totalScore)) = personScore
    (name, totalScore / numberScores)
  }

  // To get our final result we call the map function on the scores RDD passing in the averagingFunction which
  // simply calculates the average score and returns a tuple of (name,averageScore)
  val averageScores = scores.collectAsMap().map(averagingFunction)

  println("Average Scores using CombingByKey")
  averageScores.foreach(ps => {
    val(name,average) = ps
    println(name+ "'s average score : " + average)
  })
}

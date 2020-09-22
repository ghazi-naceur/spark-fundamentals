package official.doc.examples.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{EdgeContext, Graph, TripletFields, VertexId, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators

object MessageAggregationExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create a graph with "age" as the vertex property.
    // Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((id, _) => id.toDouble)
    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst((1, triplet.srcAttr))
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues( (id, value) =>
        value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))

//    def msgFun(triplet: TripletFields[Int, Float]): Iterator[(Int, String)] = {
//      Iterator((triplet.dstId, "Hi"))
//    }
//    def reduceFun(a: String, b: String): String = a + " " + b
//    val result = graph.mapReduceTriplets[String](msgFun, reduceFun)

//    def msgFun(triplet: EdgeContext[Int, Float, String]) {
//      triplet.sendToDst("Hi")
//    }
//    def reduceFun(a: String, b: String): String = a + " " + b
//    val result = graph.aggregateMessages[String](msgFun, reduceFun)

    // Define a reduce operation to compute the highest degree vertex
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    // Compute the max degrees
    val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
    println(maxInDegree._1 + " " + maxInDegree._2)

    val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
    println(maxOutDegree._1 + " " + maxOutDegree._2)

    val maxDegrees: (VertexId, Int)   = graph.degrees.reduce(max)
    println(maxDegrees._1 + " " + maxDegrees._2)
  }
}

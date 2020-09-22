package official.doc.examples.graphx

import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

class VertexProperty()
case class UserProperty(name: String) extends VertexProperty
case class ProductProperty(name: String, price: Double) extends VertexProperty

object GraphExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MyApp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Count all users which are postdocs
    val countPostDocs = graph.vertices.filter { case (_, (_, pos)) => pos == "postdoc" }.count
    println(countPostDocs)
    // Count all the edges where src > dst
    val countEdges = graph.edges.filter(e => e.srcId > e.dstId).count
    println(countEdges)

    // Use the triplets view to create an RDD of facts.
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))

    // Use the implicit GraphOps.inDegrees operator
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    inDegrees.foreach(kv => println(kv._1 + " " + kv._2))

    val outDegrees: VertexRDD[Int] = graph.outDegrees
    outDegrees.foreach(kv => println(kv._1 + " " + kv._2))

    // Given a graph where the vertex property is the out degree
    val inputGraph: Graph[Int, String] =
      graph.outerJoinVertices(graph.outDegrees)((_, _, degOpt) => degOpt.getOrElse(0))
    // Construct a graph where each edge contains the weight
    // and each vertex is the initial PageRank
    val outputGraph: Graph[Double, Double] =
    inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((_, _) => 1.0)

    val res = outputGraph.triplets.map(triplet =>
      triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr)
    res.collect.foreach(println(_))

    val degreeGraph = graph.outerJoinVertices(outDegrees) { (id, oldAttr, outDegOpt) =>
      outDegOpt match {
        case Some(outDeg) => outDeg
        case None => 0 // No outDegree means zero outDegree
      }
    }

    // Run Connected Components
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    println("ccGraph :")
    ccGraph.triplets.map(triplet =>
      triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr)

    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    println("validGraph :")
    validGraph.triplets.map(triplet =>
      triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr)

    // Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph)
    println("validCCGraph :")
    validCCGraph.triplets.map(triplet =>
      triplet.srcAttr + " is the " + triplet.attr + " of " + triplet.dstAttr)

//    val nonUniqueCosts: RDD[(VertexId, Double)]
//
//    val uniqueCosts: VertexRDD[Double] =
//      graph.vertices.aggregateUsingIndex(nonUniqueCosts, (a,b) => a + b)
//    val joinedGraph = graph.joinVertices(uniqueCosts)(
//      (id, oldCost, extraCost) => oldCost + extraCost)
//
//    val joinedGraph = graph.joinVertices(uniqueCosts, (id: VertexId, oldCost: Double, extraCost: Double) => oldCost + extraCost)
  }
}

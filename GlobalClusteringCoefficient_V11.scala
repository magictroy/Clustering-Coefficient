import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  *@version
  *     1.1
  *@author
  *     Sixun Ouyang
  *@define
  *     Compute the values of global, local and network average clustering coefficient
  *@note
  *       this version just finished the global clustering coefficient
  *            based on C(G) = 3 * number of triangles in G / 3 * number of triangles in G + open triplets of vertices
  *                          = number of closed triplets / number of connected triplets of vertices
  *                          = 3 * number of triangle in G / ∑ (1/2) * d(v) * (d(v) - 1)  [d(v) is the degree of each vertices]
  *
  *            First using the function called aggregateMessage to calculate degree for each vertices.
  *
  *            by using that degree, we can get the number of connected triplets
  *
  *            Then we compute neighbors id of each vertices.
  *
  *            Based on these neighbors, using intersect within neighbors of two vertices which related to each other.
  *
  *            According to that, we can get the number of triangles of each vertices
  *
  *            Using 3 multiplied the total triangle numbers divided by connected triplets, we can get the global clustering coefficient of this graph
  * */

object GlobalClusteringCoefficient_V11{
  def main(args: Array[String]): Unit ={
    //turn off the log information
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //set the master
    val conf = new SparkConf().setMaster("local").setAppName("CLusteringCoefficient")
    val sc = new SparkContext(conf)

    //generate the vertices and the edges
    val vertices: RDD[(VertexId, String)] = sc.parallelize(
      Array((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e"),
        (6L, "f"), (7L, "g"), (8L, "h"), (9L, "i"), (10L, "j"), (11L, "k"),
        (12L, "l"), (13L, "m"), (14L, "n"), (15L, "o"), (16L, "p")))

    val edges: RDD[Edge[String]] = sc.parallelize(
      Array(Edge(1L, 2L, "ab"), Edge(1L, 3L, "ac"), Edge(1L, 5L, "ae"), Edge(1L, 8L, "ah"), Edge(1L, 7L, "ag"),
        Edge(2L, 3L, "bc"), Edge(2L, 9L, "bi"), Edge(2L, 11L, "bk"), Edge(2L, 12L, "bl"), Edge(2L, 15L, "bo"),
        Edge(4L, 5L, "de"), Edge(4L, 6L, "df"),
        Edge(5L, 6L, "ef"),
        Edge(6L, 8L, "fh"), Edge(6L, 7L, "fg"),
        Edge(7L, 8L, "gh"),
        Edge(9L, 10L, "ij"), Edge(9L, 11L, "ik"),
        Edge(10L, 11L, "jk"),
        Edge(12L, 13L, "lm"),
        Edge(14L, 15L, "no"),
        Edge(15L, 16L, "op")))

    //calculate the global clustering coefficient
    val globalCC = globalClusteringCoefficient(vertices, edges)
  }

  def globalClusteringCoefficient(vertices: RDD[(VertexId, String)], edges: RDD[Edge[String]]): Unit = {
    //generate the graph
    val graph = Graph(vertices, edges)

    //count the degree
    val degreeCount: VertexRDD[(Double)] = graph.aggregateMessages[(Double)](
      triplet => {
        triplet.sendToSrc(1.0)
        triplet.sendToDst(1.0)
      },
      (a, b) => (a+b)
    )

    //count the total number of connected tripltes
    val triplesSum = (for (i <- degreeCount.values) yield (i * (i - 1) * 0.5)).sum()

    //count the neighbor of each vertices
    val neighbors: VertexRDD[(List[Int])] = graph.aggregateMessages[(List[Int])](
      triplet => {
        val dstId = triplet.dstId.toInt :: Nil
        val srcId = triplet.srcId.toInt :: Nil
        triplet.sendToDst(srcId)
        triplet.sendToSrc(dstId)
      },
      (a, b) => a ++ b
    )

    //generate the neighbor graph
    val neighborGraph = Graph(neighbors, edges)

    //count the number of triangles
    val neighborIntersect: VertexRDD[(Int)] = neighborGraph.aggregateMessages[(Int)](
      triplet =>{
        val triangleSet = triplet.srcAttr.toSet & triplet.dstAttr.toSet
        println("the neighbor of srcid " + triplet.srcId + " is " + triplet.srcAttr.toList)
        println("the neighbor of ,,,,,,,dstid " + triplet.dstId + " is " + triplet.dstAttr.toList)
        triplet.sendToSrc(triangleSet.size)
        triplet.sendToDst(triangleSet.size)
      },
      (a, b) => a + b
    )
    val tirangles = neighborIntersect.mapValues(x => x / 2).values.sum() / 3
    val globalClusteringCoefficient = (3 * tirangles) / triplesSum
    globalClusteringCoefficient
  }

}

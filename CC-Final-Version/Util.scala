package GraphX

import org.apache.spark.graphx._

/**
  * Created by troy on 21/02/16.
  */

class Util {
  def removeLoop(graph: Graph[Int, Int]): Graph[Int, PartitionID] = {
    val convert = graph.edges.filter{s => s.srcId != s.dstId}
    val raw_graph: Graph[Int, PartitionID] =
      Graph.fromEdges(convert, 0)
    raw_graph.cache()
  }

  def calculateCC(triplets: Graph[(Int, Int), Int], triangles: Graph[(List[Int], Int, Int), Int]):
  (Graph[(Int, Int, Double), PartitionID], Double, Double) = {
    /**
      * gathering triplets and triangles
      * */
    val triangleAndTriplets = triplets.joinVertices(triangles.vertices)((id, trip, trian) => (trian._2, trip._2))
    /**
      * calculate the local clustering coefficient of each vertex
      * */
    val localClusteringCoefficient = triangleAndTriplets.mapVertices{(id, attr) =>
      (attr._1, attr._2, if (attr._2 == 0) 0.0 else attr._1.toDouble / attr._2.toDouble)}
    val GloAveCC: (Double, Double) = GlobalAndAverageCC(localClusteringCoefficient)
    (localClusteringCoefficient, GloAveCC._1, GloAveCC._2)
  }

  /**
    * this function is to calculate the global and average clustering coefficient
    * the param of this function based on the results of previous process
    * */
  def GlobalAndAverageCC(graph: Graph[(Int, Int, Double), PartitionID]): (Double, Double) = {
    val triangles = graph.mapVertices((id, attr) => attr._1)
    val triplets = graph.mapVertices((id, attr) => attr._2)
    val localCC = graph.mapVertices((id, attr) => attr._3)

    val numTriangles = triangles.vertices.values.sum()
    val numTriplets = triplets.vertices.values.sum()
    val globalClusteringCoefficient = (3 * numTriangles) / numTriplets
    val averageClusteringCoefficient = localCC.vertices.values.sum() / localCC.vertices.values.count()
    (globalClusteringCoefficient, averageClusteringCoefficient)
  }
}
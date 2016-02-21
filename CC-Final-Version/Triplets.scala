package GraphX

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._

/**
  * Created by troy on 21/02/16.
  */

class Triplets(val graph: Graph[Int, Int]) {
  val tripletsGraph: Graph[(Int, Int), Int] =  {
    val degrees: VertexRDD[Int] = graph.degrees
    val degreeGraph = Graph(degrees, graph.edges)
    /**
      * count connected triplets
      * by the formula of
      *     G / ∑ (1/2) * d(v) * (d(v) - 1)  [d(v) is the degree of each vertices]
      * */
    degreeGraph.mapVertices((id, attr) => (attr, (attr * (attr - 1) * 0.5).toInt))
  }
}

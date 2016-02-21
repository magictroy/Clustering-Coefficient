package GraphX

import org.apache.spark.graphx._
import java.io.Serializable

/**
  * Created by troy on 21/02/16.
  */

class Triangles(val graph: Graph[Int, PartitionID]) extends Serializable{
  val triangleGraph: Graph[(List[Int], Int, Int), PartitionID] = {
    val initialGraph = graph.mapVertices((id, attr) => (List(0), 0, 0))
    /**
      * param of pregel (triangle)
      *      the first one (List[Int]) represents the neighbor of each vertices
      *      the second one (Int) represents the double times of neighbors of neighbors of a vertex
      *      the third one (Int) control the iteration time
      * */
    initialGraph.pregel((List(-1, -2, -3), Int.MaxValue, 0), activeDirection = EdgeDirection.Either)(triangleVP, triangleSM, triangleMC)
  }

  /**
    * this function is the vertexProgram of triangle pregel
    * */
  def triangleVP(id: VertexId, attr: (List[Int], Int, Int), msg: (List[Int], Int, Int)):
  (List[Int], Int, Int) = {
    val stage = attr._3 + 1
    val triangles = computeTriangle(msg._1)
    (msg._1, triangles, stage)
  }

  /**
    * count triangles crossing this vertex
    * by counting the interset neighbors of each vertex
    * because each triangles counted twice, we divided by 2
    * NOTE:
    * */
  def computeTriangle(list: List[Int]): Int = {
    if (list.equals(List(-1, -2, -3))) 0
    else list.length / 2
  }

  /**
    * this function is the sendMessage of triangle pregel
    * set the iretation time to 3. Because only 2 times iteration we can get the final results.
    * */
  def triangleSM(edge: EdgeTriplet[(List[Int], Int, Int), Int]): Iterator[(VertexId, (List[Int], Int, Int))] = {
    if (edge.srcAttr._3 < 3) {
      val neighbor: (List[Int], List[Int]) = countNeighbor(edge.srcId.toInt, edge.dstId.toInt)
      val neighborIntersect: Int = countNeighborIntersect(edge.dstAttr._1, edge.srcAttr._1)
      val src = (neighbor._1, neighborIntersect, edge.srcAttr._3)
      val dst = (neighbor._2, neighborIntersect, edge.dstAttr._3)
      val itSrc = Iterator((edge.srcId, src))
      val itDst = Iterator((edge.dstId, dst))
      itSrc ++ itDst
    } else {
      Iterator.empty
    }
  }

  /**
    * count the neighbor of each vertex
    * get neighbors by assigning destination id to source attributes
    * */
  def countNeighbor(srcId: Int, dstId: Int): (List[Int], List[Int]) = {
    val dstNil = dstId.toInt :: Nil
    val srcNil = srcId.toInt :: Nil
    (dstNil, srcNil)
  }

  /**
    * count the intersect neighbor of each vertex
    * the reason to do this process is that
    *     if b and c is neighbor of a, besides that c is also the neighbor of b, they can be a triangle
    * */
  def countNeighborIntersect(a: List[Int], b: List[Int]): Int = {
    //val triangleSet = a.toSet & b.toSet
    a.intersect(b).length
  }

  /**
    * this function is the messageCombine of triangle pregel
    * */
  def triangleMC(msg1: (List[Int], Int, Int), msg2: (List[Int], Int, Int)):
  (List[Int], Int, Int) = {
    (mergeNeighborMessage(msg1._1, msg2._1), mergeNeighborIntersect(msg1._2, msg2._2), msg1._3)
  }

  /**
    * using union process to help gather neighbors and intersect neighbors
    * */
  def mergeNeighborMessage(a: List[Int], b: List[Int]): List[Int] = {
    a ++ b
  }

  def mergeNeighborIntersect(a: Int, b: Int): Int = {
    a + b
  }
}

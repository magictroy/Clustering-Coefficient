import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * @version
  *     2.1
  * @author
  *     Sixun Ouyang
  * @define
  *     This program will compute the global, local, and average clustering coefficient of a graph
  *
  *     progress:
  *         this program will cost less memory than the previous program
  *         using pregel automatically without max iteration
  *         finished global, local and average clustering coefficient
  * */

object ClusteringCoefficient_V21{
  /**
    * main function, change the csv file path and the csv file
    *
    * param of pregel one (triplets):
    *      the first one (Int) represents the degree of each vertices
    *      the second one (Int) represents the connected triplets of each vertices
    *
    * param of pregel two (triangle)
    *      the first one (List[Int]) represents the neighbor of each vertices
    *      the second one (List[Int]) represents the intersect neighbors with neighbors of each vertices
    *      the third one (Int) represents the triangle numbers of each vertices
    * */
  def main(arge: Array[String]): Unit = {
    val beg = System.currentTimeMillis()
    /**turn off the log information and set master*/
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local").setAppName("CLusteringCoefficient")
    val sc = new SparkContext(conf)

    /**set path*/
    val path: String = "/home/troy/Clustering Coeffiency/data/s10.csv"
    val graph = loadData(path, sc)

    /**calculate the connected triangles, i.e. triplets*/
    val tripletsGraph = graph.mapVertices((id, attr) => (0, 0))
    val tripletsMap = tripletsGraph.pregel((Int.MaxValue, Int.MaxValue),
      activeDirection = EdgeDirection.Either)(tripletsVP, tripletsSM, tripletsMC)

    /**calculate the triangles*/
    val triangleGraph = graph.mapVertices((id, attr) => (List(0), List(0), 0))
    val triangleMap = triangleGraph.pregel((List(-1, -2, -3), List(-1, -2, -3), Int.MaxValue),
      activeDirection = EdgeDirection.Either)(triangleVP, triangleSM, triangleMC)

    /**gathering triplets and triangles*/
    val triangleAndTriplets = tripletsMap.joinVertices(triangleMap.vertices)((id, trip, trian) => (trian._3, trip._2))

    /**calculate the local clustering coefficient of each vertex*/
    val localClusteringCoefficient = triangleAndTriplets.mapVertices{(id, attr) =>
      (attr._1, attr._2, if (attr._2 == 0) 0.0 else attr._1.toDouble / attr._2.toDouble)}
    localClusteringCoefficient.vertices.collect().foreach(x => println("the local clustering coefficient of " + x._1 + " is " + x._2._3))

    /**calculate the global clustering coefficient and average clustering coefficient*/
    val GloAveCC: (Double, Double) = GlobalAndAverageCC(localClusteringCoefficient)
    println("The global clustering coefficient of this graph is " + GloAveCC._1)
    println("The average clustering coefficient of this graph is " + GloAveCC._2)
    println("time cost: " + (System.currentTimeMillis() - beg))
  }

  /**
    * load and convert data into a desier format
    * */
  def loadData(path: String, sc: SparkContext): Graph[Double, PartitionID] ={
    /**load from file*/
    val raw: RDD[Edge[Int]] = sc.textFile(path).map{s =>
      val parts = s.split("\\s+")
      Edge(parts(0).toLong, parts(1).toLong, 1)
    }.distinct
    val convert : RDD[Edge[Int]] = raw.filter{ s =>
      s.srcId != s.dstId
    }

    /**build graph*/
    val raw_graph : Graph[Double, PartitionID] =
      Graph.fromEdges(convert, Double.PositiveInfinity)
    raw_graph.cache()
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

  /**
    * this function is the vertexProgram of triplets pregel
    * */
  def tripletsVP(id: VertexId, attr: (Int, Int), msg: (Int, Int)): (Int, Int) = {
    val numTriplets = computeTriplets(msg._1)
    val mergedMessage = (msg._1, numTriplets)
    mergedMessage
  }

  /**
    * count connected triplets
    * by the formula of
    *     G / 鈭� (1/2) * d(v) * (d(v) - 1)  [d(v) is the degree of each vertices]
    * */
  def computeTriplets(degree: Int): Int = {
    if (degree == Int.MaxValue) Int.MaxValue
    else (0.5 * degree* (degree - 1)).toInt
  }

  /**
    * this function is the sendMessage of triplets pregel
    * */
  def tripletsSM(edge: EdgeTriplet[(Int, Int), Int]): Iterator[(VertexId, (Int, Int))] ={
    if ((edge.srcAttr._1 == Int.MaxValue) | (edge.dstAttr._1 == Int.MaxValue)) {
      val src = (1, 0)
      val dst = (1, 0)
      val itSrc = Iterator((edge.srcId, src))
      val itDst = Iterator((edge.dstId, dst))
      return itSrc ++ itDst
    }
    Iterator.empty
  }

  /**
    * this function is the messageCombine of triplets pregel
    * */
  def tripletsMC(msg1: (Int, Int), msg2: (Int, Int)): (Int, Int) ={
    val degree = mergeDegreeMessage(msg1._1, msg2._1)
    (degree, Int.MaxValue)
  }

  /**
    * calculate the degree of each vertex
    * */
  def mergeDegreeMessage(a: Int, b: Int): Int = {
    a + b
  }

  /**
    * this function is the vertexProgram of triangle pregel
    * */
  def triangleVP(id: VertexId, attr: (List[Int], List[Int], Int), msg: (List[Int], List[Int], Int)):
  (List[Int], List[Int], Int) = {
    if (attr._2 == msg._2) (msg._1, msg._2, computeTriangle(msg._2))
    else (msg._1, msg._2, msg._3)
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
    * */
  def triangleSM(edge: EdgeTriplet[(List[Int], List[Int], Int), Int]):
  Iterator[(VertexId, (List[Int], List[Int], Int))] = {
    if (edge.srcAttr._3 == Int.MaxValue) {
      val src = (countNeighbor(edge.srcId.toInt, edge.dstId.toInt)._1,
        countNeighborIntersect(edge.srcAttr._1, edge.dstAttr._1), edge.srcAttr._3)
      val dst = (countNeighbor(edge.srcId.toInt, edge.dstId.toInt)._2,
        countNeighborIntersect(edge.dstAttr._1, edge.srcAttr._1), edge.dstAttr._3)
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
  def countNeighborIntersect(srcNeigh: List[Int], dstNeigh: List[Int]): List[Int] = {
    val triangleSet = srcNeigh.toSet & dstNeigh.toSet
    triangleSet.toList
  }

  /**
    * this function is the messageCombine of triangle pregel
    * */
  def triangleMC(msg1: (List[Int], List[Int], Int), msg2: (List[Int], List[Int], Int)):
  (List[Int], List[Int], Int) = {
    (mergeNeighborMessage(msg1._1, msg2._1), mergeNeighborMessage(msg1._2, msg2._2), Int.MaxValue)
  }

  /**
    * using union process to help gather neighbors and intersect neighbors
    * */
  def mergeNeighborMessage(a: List[Int], b: List[Int]): List[Int] = {
    a ++ b
  }
}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * @version
  *     2.0
  * @author
  *     Sixun Ouyang
  * @define
  *     This program will compute the global, local, and average clustering coefficient of a graph
  *
  *     progress:
  *         load data as the meta-data
  *         using pregel function to make a parallel computing
  *         finished global, local and average clustering coefficient
  * */

object ClusteringCoefficient_V20{
  /**
    * main function, change the csv file path and the csv file
    * */
  def main(arge: Array[String]): Unit = {
    /**turn off the log information and set master*/
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local").setAppName("CLusteringCoefficient")
    val sc = new SparkContext(conf)

    /**set path*/
    val path: String = "/home/troy/Clustering Coeffiency/data/s10.csv"
    val graph = loadData(path, sc)

    /**
      * using pregel function to compute the triangle number, connected triplets number and local clustering coefficient
      *
      * param of pregel:
      *      the first one (Int) represents the degree of each vertices
      *      the second one (Int) represents the connected triplets of each vertices
      *      the third one (List[Int]) represents the neighbor of each vertices
      *      the fourth one (List[Int]) represents the intersect neighbors with neighbors of each vertices
      *      the fifth one (Int) represents the triangle numbers of each vertices
      *      the last one (Double) represents the local clustering coefficient of each vertices
      *
      *      the max iteration are set into 2 and send messges follow either sides
      * */
    val test = graph.pregel((0, 0, List(1, 2, 3), List(1, 2, 3), 0, Double.PositiveInfinity),
      maxIterations = 2, activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, messageCombiner)
    //test.vertices.collect().foreach(println(_))
    test.vertices.collect().foreach(x => println("the local clustering coefficient of " + x._1 + " is " + x._2._6))

    val GloAveCC: (Double, Double) = GlobalAndAverageCC(test)
    println("The global clustering coefficient of this graph is " + GloAveCC._1)
    println("The average clustering coefficient of this graph is " + GloAveCC._2)
  }

  /**
    * this function is to calculate the global and average clustering coefficient
    * the param of this function based on the results of previous process
    * */
  def GlobalAndAverageCC(graph: Graph[(Int, Int, List[Int], List[Int], Int, Double), PartitionID]): (Double, Double) = {
    val triplets = graph.mapVertices((id, attr) => attr._2)
    val triangles = graph.mapVertices((id, attr) => attr._5)
    val localCC = graph.mapVertices((id, attr) => attr._6)

    val numTriangles = triangles.vertices.values.sum()
    val numTriplets = triplets.vertices.values.sum()
    val globalClusteringCoefficient = (3 * numTriangles) / numTriplets
    val averageClusteringCoefficient = localCC.vertices.values.sum() / localCC.vertices.values.count()
    (globalClusteringCoefficient, averageClusteringCoefficient)
  }

  /**
    * load and convert data into a desier format
    * */
  def loadData(path: String, sc: SparkContext): Graph[(Int, Int, List[Int], List[Int], Int, Double), PartitionID] ={
    //load from file
    val raw: RDD[Edge[Int]] = sc.textFile(path).map{s =>
      val parts = s.split("\\s+")
      Edge(parts(0).toLong, parts(1).toLong, 1)
    }.distinct

    val convert : RDD[Edge[Int]] = raw.filter{ s =>
      s.srcId != s.dstId
    }

    //build graph
    val raw_graph : Graph[(Int, Int, List[Int], List[Int], Int, Double), PartitionID] =
      Graph.fromEdges(convert, (0, 0, Nil, Nil, 0, Double.PositiveInfinity))

    raw_graph.cache()
  }

  /**
    * processing messages
    * this funciton will use computeTriplets, computTriangles and computLocalCC functions
    * */
  def vertexProgram(id: VertexId, attr: (Int, Int, List[Int], List[Int], Int, Double),
                    msg: (Int, Int, List[Int], List[Int], Int, Double)):
  (Int, Int, List[Int], List[Int], Int, Double) = {
    val numTriblets = computeTriplets(msg._1)
    val numTriangles = computTriangles(msg._4)
    val localCC = computLocalCC(numTriangles, numTriblets)
    val mergedMessage = (msg._1, numTriblets, msg._3, msg._4, numTriangles, localCC)
    mergedMessage
  }

  /**
    * count connected triplets
    * by the formula of
    *     G / ∑ (1/2) * d(v) * (d(v) - 1)  [d(v) is the degree of each vertices]
    * */
  def computeTriplets(degree: Int): Int = {
    (0.5 * degree* (degree - 1)).toInt
  }

  /**
    * count triangles crossing this vertex
    * by counting the interset neighbors of each vertex
    * because each triangles counted twice, we divided by 2
    * */
  def computTriangles(list: List[Int]): Int = {
    list.length / 2
  }

  /**
    * compute local clustering coefficient
    * owing to some vertex did not have triblets, they are assigned to 0
    * */
  def computLocalCC(numTriangles: Double, numTriblets: Double): Double = {
    if (numTriblets == 0) 0.0
    else numTriangles / numTriblets
  }

  /**
    * send message
    * this function will use countNeighbor and countNeighborIntersect function
    * */
  def sendMessage(edge: EdgeTriplet[(Int, Int, List[Int], List[Int], Int, Double), Int]):
  Iterator[(VertexId, (Int, Int, List[Int], List[Int], Int, Double))] = {

    val src = (1, 0, countNeighbor(edge.srcId.toInt, edge.dstId.toInt)._1,
      countNeighborIntersect(edge.srcAttr._3, edge.dstAttr._3), edge.srcAttr._5, edge.srcAttr._6)
    val dst = (1, 0, countNeighbor(edge.srcId.toInt, edge.dstId.toInt)._2,
      countNeighborIntersect(edge.dstAttr._3, edge.srcAttr._3), edge.dstAttr._5, edge.dstAttr._6)
    val itSrc = Iterator((edge.srcId, src))
    val itDst = Iterator((edge.dstId, dst))
    itSrc ++ itDst
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
    * process message
    * this function will use mergeDegreeMessage and mergeNeighborMessage functions
    * */
  def messageCombiner(msg1: (Int, Int, List[Int], List[Int], Int, Double),
                      msg2: (Int, Int, List[Int], List[Int], Int, Double)):
  (Int, Int, List[Int], List[Int], Int, Double) ={

    val degree = mergeDegreeMessage(msg1._1, msg2._1)
    val neighbor = mergeNeighborMessage(msg1._3, msg2._3)
    val neighborIntersect = mergeNeighborMessage(msg1._4, msg2._4)

    (degree, 0, neighbor, neighborIntersect, 0, 0.0)
  }

  /**
    * calculate the degree of each vertex
    * */
  def mergeDegreeMessage(a: Int, b: Int): Int ={
    a + b
  }

  /**
    * using union process to help gather neighbors and intersect neighbors
    * */
  def mergeNeighborMessage(a: List[Int], b: List[Int]): List[Int] = {
    a ++ b
  }
}

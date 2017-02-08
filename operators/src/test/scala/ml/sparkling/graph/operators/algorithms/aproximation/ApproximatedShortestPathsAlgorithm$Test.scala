package ml.sparkling.graph.operators.algorithms.aproximation

import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.algorithms.aproximation.ApproximatedShortestPathsAlgorithm.defaultNewPath
import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance.DataMap
import org.apache.spark.graphx.util.GraphGenerators

import scala.collection.JavaConversions._
/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 07.02.17.
  */
class ApproximatedShortestPathsAlgorithm$Test(implicit sc:SparkContext)   extends MeasureTest  {
  def time[T](str: String)(thunk: => T): (T,Long) = {
    print(str + "... ")
    val t1 = System.currentTimeMillis
    val x = thunk
    val t2 = System.currentTimeMillis
    val diff=t2 - t1
    println(diff + " msecs")
    (x,diff)
  }

  "Approximated shortest paths for simple graph" should "be correctly calculated using iterative approach" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths =ApproximatedShortestPathsAlgorithm.computeShortestPathsLengthsIterative(graph, (g:Graph[_,_])=>1)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().map{
      case (vId,data)=>(vId,data.toMap)
    }.toSet
    verticesSortedById  should equal (Set(
      (1,Map(5 -> 12.0, 4 -> 9.0, 3 -> 6.0, 2 -> 1.0)),
      (2,Map(5 -> 9.0, 3 -> 1.0, 4 -> 6.0)),
      (3,Map(5 -> 6.0, 4 -> 1.0)),
      (4,Map(5 -> 1.0)),
      (5,Map())
    ))
  }

  "Single shortest paths 1 for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ApproximatedShortestPathsAlgorithm.computeSingleShortestPathsLengths(graph,1)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.map{
      case (vId,data)=>(vId,data.toMap)
    }.collect().toSet
    verticesSortedById should equal (Set((1,Map()), (2,Map()), (3,Map()), (4,Map()), (5,Map())))
  }

  "Single shortest paths 2 for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ApproximatedShortestPathsAlgorithm.computeSingleShortestPathsLengths(graph,2l)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.map{
      case (vId,data)=>(vId,data.toMap)
    }.collect().toSet
    verticesSortedById should equal (Set(
      (1,Map(2->1)),
      (2,Map()),
      (3,Map()),
      (4,Map()),
      (5,Map())
    ))
  }

  "Undirected graphs" should "be handled correctly" in{
    Given("graph")
    val filePathDirected = getClass.getResource("/graphs/5_nodes_directed")
    val filePathUndirected = getClass.getResource("/graphs/5_nodes_undirected")
    val grapDirected=loadGraph(filePathDirected.toString)
    val graphUndirected=loadGraph(filePathUndirected.toString)
    When("Loads graph")
    val shortestPathsAsUndirected=ApproximatedShortestPathsAlgorithm.computeShortestPaths(grapDirected,treatAsUndirected = true)
    val shortestPathsUndirected=ApproximatedShortestPathsAlgorithm.computeShortestPaths(graphUndirected)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPathsAsUndirected.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById should equal (shortestPathsUndirected.vertices.collect().sortBy{case (vId,data)=>vId})
  }


  "Approximation" should "not take long thant exact computing" in{
    Given("graph")
    val graph=GraphGenerators.logNormalGraph(sc,100,50)
    graph.cache();
    graph.vertices.collect()
    graph.edges.collect()
    sc.parallelize((1 to 10000)).map(_*1000).treeReduce(_+_)
    When("Computes shortest paths")
    val (_,exactTime) =time("Exact shortest paths")(ShortestPathsAlgorithm.computeShortestPaths(graph, treatAsUndirected = true))
    val (_,approximationTime) =time("Aproximated shortest paths")(ApproximatedShortestPathsAlgorithm.computeShortestPaths(graph, treatAsUndirected = true ))

    Then("Approximation should be faster")
    approximationTime should be <=(exactTime)
  }

}

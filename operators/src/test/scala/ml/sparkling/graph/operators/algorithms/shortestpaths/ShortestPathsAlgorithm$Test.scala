package ml.sparkling.graph.operators.algorithms.shortestpaths

import ml.sparkling.graph.operators.MeasureTest
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance.DataMap
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.collection.JavaConversions._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class ShortestPathsAlgorithm$Test(implicit sc:SparkContext)   extends MeasureTest  {



    "Shortest paths for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ShortestPathsAlgorithm.computeShortestPaths(graph)
    Then("Should calculate shortest paths correctly")
      val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,data)=>vId}
      verticesSortedById.map{case (vId,data)=>(vId,data.mapValues(s=>s.map(l=>l.toList)))} should equal (Array(
      (1,Map(2 -> Set(List(1d)), 3 -> Set(List(2d,2d)), 4 -> Set(List(3d,3d,2d)), 5 -> Set(List(4d,4d,3d,2d)))),
      (2,Map(3 -> Set(List(1d)), 4 -> Set(List(2d,3d)), 5 -> Set(List(3d,4d,3d)) )),
      (3,Map(4 -> Set(List(1d)), 5 -> Set(List(2d,4d)))),
      (4,Map(5 -> Set(List(1d)))),
      (5,Map())
    ))
  }


  "Shortest paths for simple graph" should "be correctly calculated using iterative approach" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths =ShortestPathsAlgorithm.computeShortestPathsLengthsIterative(graph, (g:Graph[_,_])=>1)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,data)=>vId}.map{
      case (vId,data)=>(vId,data.toMap)
    }
    verticesSortedById  should equal (Array(
      (1,Map(2 -> 1.0, 3 -> 2.0, 4 -> 3.0, 5 -> 4.0)),
      (2,Map(3 -> 1.0, 4 -> 2.0, 5 -> 3.0 )),
      (3,Map(4 -> 1.0, 5 -> 2.0)),
      (4,Map(5 -> 1.0)),
      (5,Map())
    ))
  }

  "Single shortest paths 1 for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ShortestPathsAlgorithm.computeSingleShortestPathsLengths(graph,1)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById should equal (Array(
      (1,0),
      (2,0),
      (3,0),
      (4,0),
      (5,0)
    ))
  }

  "Single shortest paths 2 for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ShortestPathsAlgorithm.computeSingleShortestPathsLengths(graph,2l)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById should equal (Array(
      (1,1),
      (2,0),
      (3,0),
      (4,0),
      (5,0)
    ))
  }



  "Undirected graphs" should "be handled correctly" in{
    Given("graph")
    val filePathDirected = getClass.getResource("/graphs/5_nodes_directed")
    val filePathUndirected = getClass.getResource("/graphs/5_nodes_undirected")
    val grapDirected=loadGraph(filePathDirected.toString)
    val graphUndirected=loadGraph(filePathUndirected.toString)
    When("Loads graph")
    val shortestPathsAsUndirected=ShortestPathsAlgorithm.computeShortestPaths(grapDirected,treatAsUndirected = true)
    val shortestPathsUndirected=ShortestPathsAlgorithm.computeShortestPaths(graphUndirected)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPathsAsUndirected.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById should equal (shortestPathsUndirected.vertices.collect().sortBy{case (vId,data)=>vId})
  }

}

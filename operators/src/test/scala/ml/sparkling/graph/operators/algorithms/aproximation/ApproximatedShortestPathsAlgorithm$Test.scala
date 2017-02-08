package ml.sparkling.graph.operators.algorithms.aproximation

import ml.sparkling.graph.operators.MeasureTest
import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 07.02.17.
  */
class AproximatedShortestPathsAlgorithm$Test(implicit sc:SparkContext)   extends MeasureTest  {



  "Aproximated shortest paths for simple graph" should "be correctly calculated using iterative approach" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths =AproximatedShortestPathsAlgorithm.computeShortestPathsLengthsIterative(graph, (g:Graph[_,_])=>1)
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

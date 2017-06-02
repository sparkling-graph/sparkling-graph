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

  "Shortest paths for simple grid graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/9_nodes_grid")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString).cache()
    When("Computes shortest paths")
    val shortestPaths = ShortestPathsAlgorithm.computeShortestPaths(graph, treatAsUndirected = true)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById = shortestPaths.vertices.collect().sortBy { case (vId, data) => vId }
    verticesSortedById.map { case (vId, data) => (vId, data.mapValues(s => s.map(l => l.toList))) }.toSet should equal(Set(
      (1,Map(7 -> Set(List(2.0, 4.0)), 8 -> Set(List(3.0, 5.0, 2.0), List(3.0, 7.0, 4.0), List(3.0, 5.0, 4.0)), 3 -> Set(List(2.0, 2.0)), 5 -> Set(List(2.0, 4.0), List(2.0, 2.0)), 4 -> Set(List(1.0)), 9 -> Set(List(4.0, 6.0, 5.0, 2.0), List(4.0, 8.0, 5.0, 4.0), List(4.0, 6.0, 3.0, 2.0), List(4.0, 8.0, 5.0, 2.0), List(4.0, 6.0, 5.0, 4.0), List(4.0, 8.0, 7.0, 4.0)), 6 -> Set(List(3.0, 5.0, 2.0), List(3.0, 5.0, 4.0), List(3.0, 3.0, 2.0)), 2 -> Set(List(1.0)))),
      (8,Map(7 -> Set(List(1.0)), 3 -> Set(List(3.0, 2.0, 5.0), List(3.0, 6.0, 5.0), List(3.0, 6.0, 9.0)), 5 -> Set(List(1.0)), 4 -> Set(List(2.0, 5.0), List(2.0, 7.0)), 9 -> Set(List(1.0)), 6 -> Set(List(2.0, 5.0), List(2.0, 9.0)), 2 -> Set(List(2.0, 5.0)), 1 -> Set(List(3.0, 4.0, 5.0), List(3.0, 2.0, 5.0), List(3.0, 4.0, 7.0)))),
      (2,Map(7 -> Set(List(3.0, 4.0, 5.0), List(3.0, 4.0, 1.0), List(3.0, 8.0, 5.0)), 8 -> Set(List(2.0, 5.0)), 3 -> Set(List(1.0)), 5 -> Set(List(1.0)), 9 -> Set(List(3.0, 8.0, 5.0), List(3.0, 6.0, 5.0), List(3.0, 6.0, 3.0)), 4 -> Set(List(2.0, 1.0), List(2.0, 5.0)), 6 -> Set(List(2.0, 5.0), List(2.0, 3.0)), 1 -> Set(List(1.0)))),
      (5,Map(7 -> Set(List(2.0, 8.0), List(2.0, 4.0)), 8 -> Set(List(1.0)), 3 -> Set(List(2.0, 6.0), List(2.0, 2.0)), 9 -> Set(List(2.0, 8.0), List(2.0, 6.0)), 4 -> Set(List(1.0)), 6 -> Set(List(1.0)), 2 -> Set(List(1.0)), 1 -> Set(List(2.0, 4.0), List(2.0, 2.0)))),
      (4,Map(7 -> Set(List(1.0)), 8 -> Set(List(2.0, 5.0), List(2.0, 7.0)), 3 -> Set(List(3.0, 2.0, 5.0), List(3.0, 6.0, 5.0), List(3.0, 2.0, 1.0)), 5 -> Set(List(1.0)), 9 -> Set(List(3.0, 8.0, 5.0), List(3.0, 8.0, 7.0), List(3.0, 6.0, 5.0)), 6 -> Set(List(2.0, 5.0)), 2 -> Set(List(2.0, 1.0), List(2.0, 5.0)), 1 -> Set(List(1.0)))),
      (9,Map(7 -> Set(List(2.0, 8.0)), 8 -> Set(List(1.0)), 3 -> Set(List(2.0, 6.0)), 5 -> Set(List(2.0, 8.0), List(2.0, 6.0)), 4 -> Set(List(3.0, 5.0, 8.0), List(3.0, 7.0, 8.0), List(3.0, 5.0, 6.0)), 6 -> Set(List(1.0)), 2 -> Set(List(3.0, 5.0, 8.0), List(3.0, 5.0, 6.0), List(3.0, 3.0, 6.0)), 1 -> Set(List(4.0, 4.0, 5.0, 8.0), List(4.0, 4.0, 5.0, 6.0), List(4.0, 2.0, 5.0, 6.0), List(4.0, 2.0, 5.0, 8.0), List(4.0, 4.0, 7.0, 8.0), List(4.0, 2.0, 3.0, 6.0)))),
      (3,Map(7 -> Set(List(4.0, 4.0, 1.0, 2.0), List(4.0, 4.0, 5.0, 6.0), List(4.0, 4.0, 5.0, 2.0), List(4.0, 8.0, 9.0, 6.0), List(4.0, 8.0, 5.0, 6.0), List(4.0, 8.0, 5.0, 2.0)), 8 -> Set(List(3.0, 5.0, 6.0), List(3.0, 5.0, 2.0), List(3.0, 9.0, 6.0)), 5 -> Set(List(2.0, 6.0), List(2.0, 2.0)), 4 -> Set(List(3.0, 1.0, 2.0), List(3.0, 5.0, 6.0), List(3.0, 5.0, 2.0)), 9 -> Set(List(2.0, 6.0)), 6 -> Set(List(1.0)), 2 -> Set(List(1.0)), 1 -> Set(List(2.0, 2.0)))),
      (6,Map(7 -> Set(List(3.0, 4.0, 5.0), List(3.0, 8.0, 5.0), List(3.0, 8.0, 9.0)), 8 -> Set(List(2.0, 5.0), List(2.0, 9.0)), 3 -> Set(List(1.0)), 5 -> Set(List(1.0)), 4 -> Set(List(2.0, 5.0)), 9 -> Set(List(1.0)), 2 -> Set(List(2.0, 5.0), List(2.0, 3.0)), 1 -> Set(List(3.0, 4.0, 5.0), List(3.0, 2.0, 5.0), List(3.0, 2.0, 3.0)))),
      (7,Map(8 -> Set(List(1.0)), 3 -> Set(List(4.0, 2.0, 5.0, 8.0), List(4.0, 2.0, 5.0, 4.0), List(4.0, 2.0, 1.0, 4.0), List(4.0, 6.0, 5.0, 4.0), List(4.0, 6.0, 9.0, 8.0), List(4.0, 6.0, 5.0, 8.0)), 5 -> Set(List(2.0, 8.0), List(2.0, 4.0)), 9 -> Set(List(2.0, 8.0)), 4 -> Set(List(1.0)), 6 -> Set(List(3.0, 9.0, 8.0), List(3.0, 5.0, 8.0), List(3.0, 5.0, 4.0)), 2 -> Set(List(3.0, 5.0, 8.0), List(3.0, 1.0, 4.0), List(3.0, 5.0, 4.0)), 1 -> Set(List(2.0, 4.0))))
    )
    )
    graph.unpersist(true)
  }

  "Shortest paths for simple ring graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_ring")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ShortestPathsAlgorithm.computeShortestPaths(graph,treatAsUndirected = true)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById.map{case (vId,data)=>(vId,data.mapValues(s=>s.map(l=>l.toList)))}.toSet should equal (Set(
        (3,Map(5 -> Set(List(2.0, 4.0)), 4 -> Set(List(1.0)), 2 -> Set(List(1.0)), 1 -> Set(List(2.0, 2.0)))),
        (5,Map(4 -> Set(List(1.0)), 2 -> Set(List(2.0, 1.0)), 3 -> Set(List(2.0, 4.0)), 1 -> Set(List(1.0)))),
        (2,Map(5 -> Set(List(2.0, 1.0)), 4 -> Set(List(2.0, 3.0)), 3 -> Set(List(1.0)), 1 -> Set(List(1.0)))),
        (4,Map(5 -> Set(List(1.0)), 3 -> Set(List(1.0)), 2 -> Set(List(2.0, 3.0)), 1 -> Set(List(2.0, 5.0)))),
        (1,Map(5 -> Set(List(1.0)), 4 -> Set(List(2.0, 5.0)), 2 -> Set(List(1.0)), 3 -> Set(List(2.0, 2.0))))
      )
    )
    graph.unpersist(true)
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
    graph.unpersist(true)
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
    graph.unpersist(true)
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
    graph.unpersist(true)
  }

  "Single shortest paths for small grid graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/9_nodes_grid")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ShortestPathsAlgorithm.computeShortestPathsLengths(graph,treatAsUndirected = true)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,data)=>vId}.map{
      case (vId,data)=>(vId,data.toMap)
    }
    verticesSortedById should equal (Array(
      (1,Map(7->2.0, 8->3.0, 3->2.0, 5->2.0, 4->1.0, 9->4.0, 6->3.0, 2->1.0)),
      (2,Map(7->3.0, 8->2.0, 3->1.0, 5->1.0, 4->2.0, 9->3.0, 6->2.0, 1->1.0)),
      (3,Map(7->4.0, 8->3.0, 5->2.0, 4->3.0, 9->2.0, 6->1.0, 2->1.0, 1->2.0)),
      (4,Map(7->1.0, 8->2.0, 3->3.0, 5->1.0, 9->3.0, 6->2.0, 2->2.0, 1->1.0)),
      (5,Map(7->2.0, 8->1.0, 3->2.0, 9->2.0, 4->1.0, 6->1.0, 2->1.0, 1->2.0)),
      (6,Map(7->3.0, 8->2.0, 3->1.0, 5->1.0, 4->2.0, 9->1.0, 2->2.0, 1->3.0)),
      (7,Map(8->1.0, 3->4.0, 5->2.0, 9->2.0, 4->1.0, 6->3.0, 2->3.0, 1->2.0)),
      (8,Map(7->1.0, 3->3.0, 5->1.0, 4->2.0, 9->1.0, 6->2.0, 2->2.0, 1->3.0)),
      (9,Map(7->2.0, 8->1.0, 3->2.0, 5->2.0, 4->3.0, 6->1.0, 2->3.0, 1->4.0))))
    graph.unpersist(true)
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
    grapDirected.unpersist(true)
    graphUndirected.unpersist(true)
  }

}

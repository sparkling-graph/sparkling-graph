package ml.sparkling.graph.operators.measures

import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class NeighborhoodConnectivity$Test(implicit sc:SparkContext)  extends MeasureTest {



  "Neighbor connectivity for directed line graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes Neighbor connectivity ")
    val result = NeighborhoodConnectivity.compute(graph)
    Then("Should calculate Neighbor connectivity  correctly")
    val verticesSortedById=result.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById .map{case (vId,data)=>data} should equal (Array(
      1d,1d,1d,0d,0d
    ))
  }

  "Neighbor connectivity for undirected line graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes Neighbor connectivity ")
    val result = NeighborhoodConnectivity.compute(graph,VertexMeasureConfiguration[Int,Int](true))
    Then("Should calculate Neighbor connectivity  correctly")
    val verticesSortedById=result.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById .map{case (vId,data)=>data} should equal (Array(
      2d,1.5,2d,1.5,2d
    ))
  }

  "Neighbor connectivity for full 4 node directed graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes Neighbor connectivity")
    val result=NeighborhoodConnectivity.compute(graph)
    Then("Should calculate Neighbor connectivity correctly")
    val verticesSortedById=result.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById .map{case (vId,data)=>data} should equal (Array(
      1d,1d,2d,1.5
    ))
  }

  "Neighbor connectivity for full 4 node undirected graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes Neighbor connectivity")
    val result=NeighborhoodConnectivity.compute(graph,VertexMeasureConfiguration[Int,Int](true))
    Then("Should calculate Neighbor connectivity correctly")
    val verticesSortedById=result.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById .map{case (vId,data)=>data} should equal (Array(
      3d,3d,3d,3d
    ))
  }

}

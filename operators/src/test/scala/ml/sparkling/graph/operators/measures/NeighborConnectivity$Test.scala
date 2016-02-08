package ml.sparkling.graph.operators.measures

import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.SparkTest
import ml.sparkling.graph.operators.measures.hits.Hits
import org.apache.spark.graphx.Graph
import org.scalatest.FunSuite

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class NeighborConnectivity$Test extends SparkTest {

  def appName = "neighbor-connectivity-test"


  "Neighbor connectivity for directed line graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes Neighbor connectivity ")
    val result = NeighborConnectivity.compute(graph)
    Then("Should calculate Neighbor connectivity  correctly")
    result.vertices.collect().sortBy(t => t._1).map(_._2) should equal (Array(
      1.,1.,1.,0.,0.
    ))
  }

  "Neighbor connectivity for undirected line graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes Neighbor connectivity ")
    val result = NeighborConnectivity.compute(graph,VertexMeasureConfiguration[Int,Int](true))
    Then("Should calculate Neighbor connectivity  correctly")
    result.vertices.collect().sortBy(t => t._1).map(_._2) should equal (Array(
      2.,1.5,2.,1.5,2.
    ))
  }

  "Neighbor connectivity for full 4 node directed graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes Neighbor connectivity")
    val result=NeighborConnectivity.compute(graph)
    Then("Should calculate Neighbor connectivity correctly")
    result.vertices.collect().sortBy(t => t._1).map(_._2) should equal (Array(
      1.,1.,2.,1.5
    ))
  }

  "Neighbor connectivity for full 4 node undirected graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes Neighbor connectivity")
    val result=NeighborConnectivity.compute(graph,VertexMeasureConfiguration[Int,Int](true))
    Then("Should calculate Neighbor connectivity correctly")
    result.vertices.collect().sortBy(t => t._1).map(_._2) should equal (Array(
      3.,3.,3.,3.
    ))
  }

}

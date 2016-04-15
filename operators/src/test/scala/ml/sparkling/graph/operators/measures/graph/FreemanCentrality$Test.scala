package ml.sparkling.graph.operators.measures.graph

import ml.sparkling.graph.operators.MeasureTest
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class FreemanCentrality$Test (implicit sc:SparkContext)   extends MeasureTest  {

  "Freeman Centrality  for star graph" should "be 1" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/6_nodes_star")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes Freemans Centrality")
    val result=FreemanCentrality.compute(graph)
    Then("Should calculate Freemans Centrality")
    result should be (1)
  }

  "Freeman Centrality  for star graph" should "be 1 when calculated using DSL" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/6_nodes_star")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes Freemans Centrality")
    val result=graph.freemanCentrality()
    Then("Should calculate Freemans Centrality")
    result should be (1)
  }


  "Freeman Centrality  for 5 node line graph" should "be 0.167" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes Freemans Centrality")
    val result=FreemanCentrality.compute(graph)
    Then("Should calculate Freemans Centrality")
    result should be (0.16666666 +- 1e-5)
  }


}

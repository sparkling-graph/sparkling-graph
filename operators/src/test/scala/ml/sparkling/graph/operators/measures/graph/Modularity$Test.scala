package ml.sparkling.graph.operators.measures.graph

import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import ml.sparkling.graph.operators.OperatorsDSL._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class Modularity$Test (implicit sc:SparkContext)   extends MeasureTest{

  "Modularity  for star graph" should "be 1" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/6_nodes_star")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    val graphComponents=graph.PSCAN(epsilon = 0)
    When("Computes Freemans Centrality")
    val result=Modularity.compute(graphComponents)
    Then("Should calculate Freemans Centrality")
    result should be (1)
    graph.unpersist(true)
  }

  "Modularity  for star graph" should "be 1 when calculated using DSL" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/6_nodes_star")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    val graphComponents=graph.PSCAN(epsilon = 0)
    When("Computes Freemans Centrality")
    val result=graphComponents.modularity()
    Then("Should calculate Freemans Centrality")
    result should be (1)
    graph.unpersist(true)
  }

  "Modularity  for all single components" should "be -1 " in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/6_nodes_star")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    val graphComponents=graph.PSCAN(epsilon=1)
    When("Computes Freemans Centrality")
    val result=graphComponents.modularity()
    Then("Should calculate Freemans Centrality")
    result should be (-1)
    graph.unpersist(true)
  }


}

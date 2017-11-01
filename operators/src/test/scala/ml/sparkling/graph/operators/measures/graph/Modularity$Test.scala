package ml.sparkling.graph.operators.measures.graph

import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.spark.graphx.util.GraphGenerators

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class Modularity$Test (implicit sc:SparkContext)   extends MeasureTest{

  "Modularity  for star graph in one community" should "be 0" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/6_nodes_star")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    val graphComponents=graph.PSCAN(epsilon = 0)
    When("Computes Modularity")
    val result=Modularity.compute(graphComponents)
    Then("Should calculate Modularity")
    result should be (0)
    graph.unpersist(true)
  }


  "Modularity  for ring graph in one  community" should "be 0" in{
    Given("graph")
    val graph=GraphGenerators.gridGraph(sc,5,5).mapEdges((_)=>1).mapVertices((_,_)=>1)
    val graphComponents=graph.PSCAN(epsilon = 0)
    When("Computes Modularity")
    val result=Modularity.compute(graphComponents)
    Then("Should calculate Modularity")
    result should be (0)
    graph.unpersist(true)
  }

  "Modularity  for ring graph in one node communities" should "be -0.041875" in{
    Given("graph")
    val graph=GraphGenerators.gridGraph(sc,5,5)
    val graphComponents=graph.PSCAN(epsilon = 1)
    When("Computes Modularity")
    val result=Modularity.compute(graphComponents)
    Then("Should calculate Modularity")
    result should be (-0.041875 +- 0.000000001)
    graph.unpersist(true)
  }

  "Modularity  for star graph in one community" should "be 0 when calculated using DSL" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/6_nodes_star")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    val graphComponents=graph.PSCAN(epsilon = 0)
    When("Computes Modularity")
    val result=graphComponents.modularity()
    Then("Should calculate Modularity")
    result should be (0)
    graph.unpersist(true)
  }

  "Modularity  for all single components" should "be -1 " in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/6_nodes_star")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    val graphComponents=graph.PSCAN(epsilon=1)
    When("Computes Modularity")
    val result=graphComponents.modularity()
    Then("Should calculate Modularity")
    result should be (-0.3 +- 0.000000001)
    graph.unpersist(true)
  }


}

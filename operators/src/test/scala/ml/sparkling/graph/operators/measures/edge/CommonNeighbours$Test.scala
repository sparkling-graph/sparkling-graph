package ml.sparkling.graph.operators.measures.edge

import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.OperatorsDSL._
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class CommonNeighbours$Test (implicit sc:SparkContext)   extends MeasureTest {


  "Common neighbours for star graph" should "be 0 for each node" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/6_nodes_star")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes common neighbours")
    val result=CommonNeighbours.computeWithPreprocessing(graph)
    Then("Should calculate common neighbours")
    val resultValues=result.edges.map(_.attr).distinct().collect()
    resultValues(0) should equal(0)
    resultValues.size should equal(1)
  }

  "Common neighbours for full graph using DSL" should "be 2 for each node" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes common neighbours")
    val result=graph.commonNeighbours(true)
    Then("Should calculate common neighbours")
    val resultValues=result.edges.map(_.attr).distinct().collect()
    resultValues(0) should equal(2)
    resultValues.size should equal(1)
  }


}

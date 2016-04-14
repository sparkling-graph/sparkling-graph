package ml.sparkling.graph.operators.algorithms.community.pscan

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.OperatorsDSL._
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class PSCAN$Test (implicit sc:SparkContext)   extends MeasureTest {

  "Components for full graph" should  " be computed" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes components")
    val components: Graph[ComponentID, Int] = PSCAN.computeConnectedComponents(graph)
    Then("Should compute components correctly")
    components.vertices.map{case (vId,cId)=>cId}.distinct().collect().size  should equal (1)
  }


  "Components for full graph" should  " be computed using DSL" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes components")
    val components: Graph[ComponentID, Int] =graph.PSCAN()
    Then("Should compute components correctly")
    components.vertices.map{case (vId,cId)=>cId}.distinct().collect().size  should equal (1)
  }

  "Components for ring graph" should  " be computed" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes components")
    val components: Graph[ComponentID, Int] = PSCAN.computeConnectedComponents(graph)
    Then("Should compute components correctly")
    components.vertices.map{case (vId,cId)=>cId}.distinct().collect().size  should equal (5)
  }

}

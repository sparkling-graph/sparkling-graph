package ml.sparkling.graph.operators.measures.vertex.hits

import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.OperatorsDSL._
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class Hits$Test(implicit sc:SparkContext)  extends MeasureTest  {



  "Hits  for line graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes Hits")
    val result = Hits.computeBasic(graph)
    Then("Should calculate hits correctly")
    result.vertices.collect().sortBy{case (vId,data)=>vId}.map{case (vId,data)=>data}.zip(Array(
      (0.25,0d), (0.25,0.25),(0.25,0.25),(0.25,0.25),(0d,0.25)
    )).foreach {
      case ((a,b),(c,d)) => {
        a should be (c +- 1e-5)
        b should be (d +- 1e-5)
      }
    }
    graph.unpersist(true)
  }

  "Hits  for line graph" should "be correctly calculated using DSL" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes Hits")
    val result = graph.hits()
    Then("Should calculate hits correctly")
    result.vertices.collect().sortBy{case (vId,data)=>vId}.map{case (vId,data)=>data}.zip(Array(
      (0.25,0d), (0.25,0.25),(0.25,0.25),(0.25,0.25),(0d,0.25)
    )).foreach {
      case ((a,b),(c,d)) => {
        a should be (c +- 1e-5)
        b should be (d +- 1e-5)
      }
    }
    graph.unpersist(true)
  }

  "Hits for full 4 node directed graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes Hits")
    val result=Hits.computeBasic(graph)
    Then("Should calculate Hits correctly")
    result.vertices.collect().sortBy{case (vId,data)=>vId}.map{case (vId,data)=>data}.zip(Array(
      (0.44504187450168503,0.19806226306818242),
      (0.19806226497496957,0.4450418674109515),
      (1.9336832073590722e-13,0.3568958695205176),
      (0.35689586676523016,3.484376742610991e-13)
    )).foreach {
      case ((a,b),(c,d)) => {
        a should be (c +- 1e-5)
        b should be (d +- 1e-5)
      }
    }
    graph.unpersist(true)
  }



}

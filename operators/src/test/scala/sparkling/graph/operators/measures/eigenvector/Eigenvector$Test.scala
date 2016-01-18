package sparkling.graph.operators.measures.eigenvector

import org.apache.spark.graphx.{PartitionStrategy, GraphLoader, Graph}
import sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import sparkling.graph.operators.SparkTest

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class Eigenvector$Test extends SparkTest{

  def appName = "eigenvector-test"


  "Eigenvector  for line graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes eigenvector")
    val result=Eigenvector.compute(graph)
    Then("Should calculate eigenvector correctly")
    result.vertices.collect().sortBy(t=>t._1).map(_._2).zip(Array(
      0., 0., 0., 0., 0.
    )).foreach{case (a,b)=>{a should be (b +- 1e-5 )}}
  }

  "Eigenvector  for full 4 node directed graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes eigenvector")
    val result=Eigenvector.compute(graph)
    Then("Should calculate eigenvector correctly")
    result.vertices.collect().sortBy(t=>t._1).map(_._2).zip(Array(
      0.32128186442503776, 0.5515795539542094, 0.6256715148839718, 0.44841176915201825
    )).foreach{case (a,b)=>{a should be (b +- 1e-5 )}}
  }

  "Eigenvector  for full 4 node undirected graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes eigenvector")
    val result=Eigenvector.compute(graph,VertexMeasureConfiguration[Int,Int](true))
    Then("Should calculate eigenvector correctly")
    result.vertices.collect().sortBy(t=>t._1) should equal (Array(
      (1,0.5), (2,0.5), (3,0.5), (4,0.5)
    ))
  }



}
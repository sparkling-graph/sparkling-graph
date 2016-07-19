package ml.sparkling.graph.utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 18.07.16.
  */
class Coalesce$Test(implicit sc:SparkContext )extends UtilsTest {

  "One vertex graph " should  " be result of star coalescing" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/6_nodes_star.txt")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Graph coalescing")
    val coalescedGraph = Coalesce.coalesceGraph(graph)
    Then("Should coalesce correctly")
    coalescedGraph.numVertices should equal(1)
    coalescedGraph.numEdges should equal(0)
  }

  "Two vertex graph " should  " be result of line coalescing" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed.txt")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Graph coalescing")
    val coalescedGraph = Coalesce.coalesceGraph(graph)
    Then("Should coalesce correctly")
    coalescedGraph.numVertices should equal(3)
    coalescedGraph.numEdges should equal(2)
  }

}

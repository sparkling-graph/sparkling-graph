package ml.sparkling.graph.operators.algorithms.link

import ml.sparkling.graph.operators.MeasureTest
import ml.sparkling.graph.operators.measures.edge.CommonNeighbours
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.OperatorsDSL._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class BasicLinkPredictor$Test (implicit sc:SparkContext) extends MeasureTest {

  "In open triad" should  " propose to close it" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/3_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes new links")
    val links = BasicLinkPredictor.predictLinks(graph,CommonNeighbours,0,true)
    Then("Should compute links correctly")
   links.collect() should equal(Array((1,3)))
  }

  "In open 4 nodes graph" should  " propose to close it fully" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_open")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes new links")
    val links = graph.predictLinks(CommonNeighbours,1,true)
    Then("Should compute links correctly")
    links.collect() should equal(Array((1,3),(2,4)))
  }
}

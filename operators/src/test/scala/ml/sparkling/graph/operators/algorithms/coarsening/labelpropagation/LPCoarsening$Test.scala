package ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation

import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.Component
import ml.sparkling.graph.operators.MeasureTest
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 06.02.17.
  */
class LPCoarsening$Test  (implicit sc:SparkContext)   extends MeasureTest {

  "Four node full graph " should  " be coarsed to  one node" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = LPCoarsening.coarse(graph,true)
    Then("Should compute components correctly")
    components.vertices.count()  should equal (1)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((4,List(1,2,3,4))))
    components.edges.collect().toSet should equal(Set())
  }

  "Three component graph " should  " be coarsed to three nodes graph" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/coarsening_to_3")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = graph.LPCoarse(true);
    Then("Should compute components correctly")
    components.vertices.count()  should equal (3)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((8,List(5, 6, 7, 8)), (12,List(9, 10, 11, 12)), (4,List(1, 2, 3, 4))))
    components.edges.collect().toSet should equal(Set(Edge(4,8,1), Edge(4,12,1), Edge(8,12,1)))
  }

}

package ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation

import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.Component
import ml.sparkling.graph.operators.MeasureTest
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Edge, Graph}

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 06.02.17.
  */
class LPCoarsening$Test  (implicit sc:SparkContext)   extends MeasureTest {

  "Three nodes directed graph with pair loop " should  " be coarsed to  two node" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/3_nodes_with_pair_loop_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = graph.LPCoarse()
    Then("Should compute components correctly")
    components.vertices.count()  should equal (2)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((1,List(1, 2)),(3,List(3))) )
    components.edges.collect().toSet should equal(Set(Edge(1,3,1), Edge(3,1,1)))
  }


  "Three node directed  full graph treated as undirected" should  " be coarsed to  one node" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/3_nodes_full_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = graph.LPCoarse(true)
    Then("Should compute components correctly")
    components.vertices.count()  should equal (1)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((1,List(1,2,3))))
    components.edges.collect().toSet should equal(Set())
  }


  "Four node directed ring graph " should  " be coarsed to  one node" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = LPCoarsening.coarse(graph,true)
    Then("Should compute components correctly")
    components.vertices.count()  should equal (1)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((1,List(1,2,3,4))))
    components.edges.collect().toSet should equal(Set())
  }


  "Four node directed full graph treated as undirected " should  " be coarsed to  one node" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = LPCoarsening.coarse(graph,true)
    Then("Should compute components correctly")
    components.vertices.count()  should equal (1)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((1,List(1,2,3,4))))
    components.edges.collect().toSet should equal(Set())
  }



  "Three node directed line graph without pair loops " should  " be coarsed to  two node graph" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/3_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = graph.LPCoarse()
    Then("Should compute components correctly")
    components.vertices.count()  should equal (2)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((1,List(1, 2)),(3,List(3))))
    components.edges.collect().toSet should equal(Set(Edge(1,3,1)) )
  }


  "Three node line graph treated as undirected " should  " be coarsed to  two node" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/3_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = graph.LPCoarse(true)
    Then("Should compute components correctly")
    components.vertices.count()  should equal (2)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((1,List(1,2)),(3,List(3))))
    components.edges.collect().toSet should equal(Set(Edge(1,3,1)))
  }


  "Three node directed line graph descending  " should  " be coarsed to  three node" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/3_nodes_directed_asc")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = graph.LPCoarse()
    Then("Should compute components correctly")
    components.vertices.count()  should equal (3)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((3,List(3)), (1,List(1)), (2,List(2))))
    components.edges.collect().toSet should equal(Set(Edge(2,1,1), Edge(3,2,1)))
  }



  "Five node directed ring graph  " should  " be coarsed to  four node" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_ring")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = graph.LPCoarse()
    Then("Should compute components correctly")
    components.vertices.count()  should equal (3)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((1,List(1, 2)), (3,List(3, 4)), (5,List(5))))
    components.edges.collect().toSet should equal(Set(Edge(1,3,1), Edge(3,5,1), Edge(5,1,1)) )
  }



  "Directed treated as undirected" should  " be coarsed same way as undirected" in{
    Given("graph")
    val directed = getClass.getResource("/graphs/3_nodes_directed")
    val undirected = getClass.getResource("/graphs/3_nodes_undirected")
    val directedGraph:Graph[Int,Int]=loadGraph(directed.toString)
    val undirectedGraph:Graph[Int,Int]=loadGraph(undirected.toString)
    When("Computes coarsed graph")
    val directedComponents: Graph[Component, Int] = directedGraph.LPCoarse(true)
    val undirectedComponents: Graph[Component, Int] = undirectedGraph.LPCoarse()
    Then("Should compute components correctly")
    directedComponents.vertices.count()  should equal (undirectedComponents.vertices.count())
    directedComponents.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (undirectedComponents.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet)
    undirectedComponents.edges.collect().toSet should equal(undirectedComponents.edges.collect().toSet)
  }


  "Three component directed graph without pair loops " should  " be coarsed to three nodes graph " in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/coarsening_to_3")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = graph.LPCoarse();
    Then("Should compute components correctly")
    components.vertices.count()  should equal (3)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((1,List(1, 2, 3, 4)), (9,List(9, 10, 11, 12)), (5,List(5, 6, 7, 8))))
    components.edges.collect().toSet should equal(Set(Edge(1,5,1), Edge(1,9,1), Edge(5,9,1)))
  }

  "Three component directed graph treated as undirected " should  " be coarsed to three nodes graph" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/coarsening_to_3")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = graph.LPCoarse(true);
    Then("Should compute components correctly")
    components.vertices.count()  should equal (3)
    components.vertices.collect().map{
      case (vId,component)=>(vId,component.sorted)
    }.toSet should equal (Set((1,List(1, 2, 3, 4)), (9,List(9, 10, 11, 12)), (5,List(5, 6, 7, 8))))
    components.edges.collect().toSet should equal(Set(Edge(1,5,1), Edge(1,9,1), Edge(5,9,1)))
  }


  "Big star  " should  " be coarsed to one node graph" in{
    Given("graph")
    val graph:Graph[Int,Int]=GraphGenerators.starGraph(sc,1000)
    When("Computes coarsed graph")
    val components: Graph[Component, Int] = graph.LPCoarse(true);
    Then("Should compute components correctly")
    components.vertices.count()  should equal (1)
  }


  "Grid graph " should  " be coarsed at  half" in{
    Given("graph")
    val graph=GraphGenerators.gridGraph(sc,20,20)
    When("Computes coarsed graph")
    val components= graph.LPCoarse(true);
    Then("Should compute components correctly")
    println(s"Coarsed to ${components.vertices.count()}")
    components.vertices.count()  should equal (graph.vertices.count()/2)
  }

  "Random log normal graph " should  " be coarsed at least by 40%" in{
    for (x<-0 to 10){
      Given("graph")
      val graph=GraphGenerators.logNormalGraph(sc,60)
      val expectedSize=(graph.vertices.count()*0.6).toLong
      When("Computes coarsed graph")
      val components= graph.LPCoarse(true);
      Then("Should compute components correctly")
      println(s"Coarsed to ${components.vertices.count()}")
      components.vertices.count()  should be <=(expectedSize)
    }
  }

}

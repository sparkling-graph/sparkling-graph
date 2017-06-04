package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.generators.RandomNumbers.{RandomNumberGeneratorProvider, ScalaRandomNumberGenerator}
import ml.sparkling.graph.generators.wattsandstrogatz.{WattsAndStrogatzGenerator, WattsAndStrogatzGeneratorConfiguration}
import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.scalatest.tagobjects.Slow

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class PSCANBasedPartitioning$Test(implicit sc:SparkContext) extends MeasureTest {


  "One component graph " should  " have four partitions" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] = PSCANBasedPartitioning.partitionGraphBy(graph,4)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (4)
    graph.unpersist(true)
  }

  "One component graph " should  " have four partitions when calculated using DSL" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] = PSCANBasedPartitioning.partitionGraphBy(graph,1)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (1)
    graph.unpersist(true)
  }

  "Five component graph " should  " have five partitions" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] =PSCANBasedPartitioning.partitionGraphBy(graph,5)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (5)
    graph.unpersist(true)
  }

  "Dynamic partitioning for random graph" should  " be computed" in{
    Given("graph")
    val graph:Graph[Int,Int]=GraphGenerators.rmatGraph(sc,1000,10000)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] =PSCANBasedPartitioning.partitionGraphBy(graph,24)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (24)
    graph.unpersist(true)
  }


  ignore should "Dynamic partitioning for random graph be computed in apropriate time"  taggedAs(Slow) in{
    for (x<-0 to 3) {
      logger.info(s"Run $x")
      Given("graph")
      val graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, 10000, 500000).cache()
      When("Partition using PSCAN")
      val (partitionedGraph, partitioningTime): (Graph[Int, Int], Long) = time("Partitioning")(PSCANBasedPartitioning.partitionGraphBy(graph, 24))
      Then("Should compute partitions correctly")
      partitionedGraph.edges.partitions.size should equal(24)
      partitioningTime should be < (50000l)
      graph.unpersist(true)
      partitionedGraph.unpersist(true)
    }
  }

  "Dynamic partitioning for WATS graph" should  " be computed" in{
    Given("graph")
    val seededRandomNumberGeneratorProvider:RandomNumberGeneratorProvider=(givenSeed:Long)=>{
      ScalaRandomNumberGenerator(givenSeed+1)
    }
    val graph:Graph[Int,Int]=WattsAndStrogatzGenerator.generate(WattsAndStrogatzGeneratorConfiguration(32,4,0.8,true,seededRandomNumberGeneratorProvider))
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] =PSCANBasedPartitioning.partitionGraphBy(graph,24)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (24)
    graph.unpersist(true)
  }

}

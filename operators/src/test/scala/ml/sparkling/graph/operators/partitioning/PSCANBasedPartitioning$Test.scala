package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.generators.RandomNumbers.{RandomNumberGeneratorProvider, ScalaRandomNumberGenerator}
import ml.sparkling.graph.generators.wattsandstrogatz.{WattsAndStrogatzGenerator, WattsAndStrogatzGeneratorConfiguration}
import ml.sparkling.graph.operators.MeasureTest
import ml.sparkling.graph.operators.measures.vertex.eigenvector.EigenvectorCentrality
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
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
    partitionedGraph.triplets.partitions.size  should equal (4)
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
    partitionedGraph.triplets.partitions.size  should equal (1)
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
    partitionedGraph.triplets.partitions.size  should equal (5)
    graph.unpersist(true)
  }

  "Dynamic partitioning for random graph" should  " be computed" in{
    Given("graph")
    val graph:Graph[Int,Int]=GraphGenerators.rmatGraph(sc,65536,65536*8)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] =PSCANBasedPartitioning.partitionGraphBy(graph,24)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (24)
    partitionedGraph.triplets.partitions.size  should equal (24)
    graph.unpersist(true)
  }


  ignore should "Dynamic partitioning for random graph be computed in apropriate time"  taggedAs(Slow) in{
    for (x<-0 to 3) {
      logger.info(s"Run $x")
      Given("graph")
      val graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, 65536, 65536*8).cache()
      When("Partition using PSCAN")
      val (partitionedGraph, partitioningTime): (Graph[Int, Int], Long) = time("Partitioning")(PSCANBasedPartitioning.partitionGraphBy(graph, 24))
      Then("Should compute partitions correctly")
      partitionedGraph.edges.partitions.size should equal(24)
      partitionedGraph.triplets.partitions.size  should equal (24)
      partitioningTime should be < (50000l)
      graph.unpersist(true)
      partitionedGraph.unpersist(true)
    }
  }

  "Dynamic partitioning for WATS graph" should  " be computed" in{
    Given("graph")
    val seededRandomNumberGeneratorProvider:RandomNumberGeneratorProvider=(givenSeed:Long)=>{
      ScalaRandomNumberGenerator(givenSeed+1024)
    }
    val graph:Graph[Int,Int]=WattsAndStrogatzGenerator.generate(WattsAndStrogatzGeneratorConfiguration(65536,8,0.5,true,seededRandomNumberGeneratorProvider)).cache()
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] =PSCANBasedPartitioning.partitionGraphBy(graph,24)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (24)
    partitionedGraph.triplets.partitions.size  should equal (24)
    graph.unpersist(true)
  }


  ignore should "Dynamic partitioning for random graph give faster results for eignevector"  taggedAs(Slow) in{
    Given("graph")
    val graphInit: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, 50000, 1050000).partitionBy(EdgePartition2D,8)
    val graph=Graph.fromEdges(graphInit.edges,0).cache()
    val partitionedGraph= PSCANBasedPartitioning.partitionGraphBy(graph, 8).cache()
    graph.edges.foreachPartition((_)=>{})
    graph.vertices.foreachPartition((_)=>{})
    for (x<-0 to 3) {
      logger.info(s"Run $x")
      graph.degrees
      When("Partition using PSCAN")
      val (_,computationTime)=time("Eigenvector for standard partitioning")(EigenvectorCentrality.compute(graph).vertices.foreachPartition((_)=>{}))
      val (_,computationTimeCustom)=time("Eigenvector for custom partitioning")(EigenvectorCentrality.compute(partitionedGraph).vertices.foreachPartition((_)=>{}))
      Then("Should compute partitions correctly")
      computationTimeCustom should be < (computationTime)
      graph.unpersist(true)
      partitionedGraph.unpersist(true)
      graphInit.unpersist(true)
    }
  }
}

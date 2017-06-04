package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.scalatest.tagobjects.Slow

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class PropagationBasedPartitioning$Test(implicit sc:SparkContext) extends MeasureTest {


  "4 vertex Graph partitioned to 4 partions " should  " be partitooned to 4 partitions" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] = PropagationBasedPartitioning.partitionGraphBy(graph,4)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (4)
    graph.unpersist(true)
  }

  "4 vertex Graph partitioned to 1 partion" should  " be partitooned to 4 partition" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] = PropagationBasedPartitioning.partitionGraphBy(graph,1)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (1)
    graph.unpersist(true)
  }


  "Three component graph partitioned to three partitions " should  " have six partitions" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/coarsening_to_3")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] = PropagationBasedPartitioning.partitionGraphBy(graph,3)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (3)
    graph.unpersist(true)
  }

  "Dynamic partitioning for random graph" should  " be computed" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/coarsening_to_3")
    val graph:Graph[Int,Int]=GraphGenerators.rmatGraph(sc,1000,10000)
    When("Partition using Propagation method")
    val partitionedGraph: Graph[Int, Int] =PropagationBasedPartitioning.partitionGraphBy(graph,24)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (24)
    graph.unpersist(true)
  }

  ignore should  "Dynamic partitioning for random graph   be computed in apropriate time"  taggedAs(Slow) in{
    for (x<-0 to 3) {
      logger.info(s"Run $x")
      Given("graph")
      val graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, 10000, 500000).cache()
      When("Partition using PSCAN")
      val (partitionedGraph, partitioningTime): (Graph[Int, Int], Long) = time("Partitioning")(PropagationBasedPartitioning.partitionGraphBy(graph, 24))
      Then("Should compute partitions correctly")
      partitionedGraph.edges.partitions.size should equal(24)
      partitioningTime should be < (10000l)
      graph.unpersist(true)
      partitionedGraph.unpersist(true)
    }
  }
}

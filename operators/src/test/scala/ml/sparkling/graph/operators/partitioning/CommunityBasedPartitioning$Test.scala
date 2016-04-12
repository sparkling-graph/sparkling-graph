package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.operators.MeasureTest
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class CommunityBasedPartitioning$Test(implicit sc:SparkContext) extends MeasureTest {

  "One component graph " should  " have one partition" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] = CommunityBasedPartitioning.partitionGraphBy(graph,PSCAN)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (1)
  }

  "Five component graph " should  " have five partitions" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] = CommunityBasedPartitioning.partitionGraphBy(graph,PSCAN)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (5)
  }

  "Change of community method parammeters" should  " be possible" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using PSCAN")
    val partitionedGraph: Graph[Int, Int] = CommunityBasedPartitioning.partitionGraphBy(graph,PSCAN.computeConnectedComponents(_,epsilon = 0))
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (1)
  }
}

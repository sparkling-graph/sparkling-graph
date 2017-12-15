package ml.sparkling.graph.operators.partitioning

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
class PropagationBasedPartitioning$Test(implicit sc:SparkContext) extends MeasureTest {


  "4 vertex Graph partitioned to 4 partions " should  " be partitooned to 4 partitions" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using propagation")
    val partitionedGraph: Graph[Int, Int] = PropagationBasedPartitioning.partitionGraphBy(graph,4)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (4)
      partitionedGraph.triplets.partitions.size  should equal (4)
    graph.unpersist(true)
  }

  "4 vertex Graph partitioned to 1 partion" should  " be partitooned to 4 partition" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using propagation")
    val partitionedGraph: Graph[Int, Int] = PropagationBasedPartitioning.partitionGraphBy(graph,1)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (1)
    partitionedGraph.triplets.partitions.size  should equal (1)
    graph.unpersist(true)
  }


  "Three component graph partitioned to three partitions " should  " have six partitions" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/coarsening_to_3")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Partition using propagation")
    val partitionedGraph: Graph[Int, Int] = PropagationBasedPartitioning.partitionGraphBy(graph,3)
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (3)
    partitionedGraph.triplets.partitions.size  should equal (3)
    graph.unpersist(true)
  }

  "Dynamic partitioning for random graph" should  " be computed" in{
    Given("graph")
    val graph:Graph[Int,Int]=GraphGenerators.rmatGraph(sc,1000,10000)
    When("Partition using Propagation method")
    val partitionedGraph: Graph[Int, Int] =PropagationBasedPartitioning.partitionGraphBy(graph,24).cache()
    Then("Should compute partitions correctly")
    partitionedGraph.edges.partitions.size  should equal (24)
    partitionedGraph.triplets.partitions.size  should equal (24)
    graph.edges.count() should  equal (partitionedGraph.edges.count())
    graph.vertices.count() should  equal (partitionedGraph.vertices.count())
    graph.triplets.count() should  equal (partitionedGraph.triplets.count())
    graph.unpersist(true)
    partitionedGraph.unpersist(true)
  }

    ignore should "Dynamic partitioning should  be balanced for RMAT graph" in { //TODO: Data balance can be improved!
      for (_ <- 0 to 3) {
        Given("graph")
        val size = 65550
        val meanDegree = 8
        val graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, size, meanDegree * size)
        When("Partition using Propagation method")
        val partitionedGraph: Graph[Int, Int] = PropagationBasedPartitioning.partitionGraphBy(graph, 945).cache()
        val partitionsSizes = partitionedGraph.triplets.mapPartitions(d => Iterator(d.size)).collect().toList
        val meanSize = partitionsSizes.sum / partitionsSizes.size.toDouble
        val variance = partitionsSizes.map(n => Math.pow(n - meanSize, 2)).sum / (partitionsSizes.size - 1).toDouble
        val stdDev = Math.sqrt(variance)
        Then("Should compute partitions correctly")
        logger.error(s"STDDev: ${stdDev}")
        //stdDev should be <= 10.0
      }
    }

      ignore should "Dynamic partitioning should be balanced for WATS graph" in{  //TODO: Data balance can be improved!
        for (_<-0 to 3) {
          Given("graph")
          val size = 65550
          val meanDegree = 8
          val sigma=1.3
          val mu=Math.log(meanDegree)-(sigma*sigma)/2.0
          val graph = GraphGenerators.logNormalGraph(sc, size,mu=mu, sigma=sigma )
          When("Partition using Propagation method")
          val partitionedGraph = PropagationBasedPartitioning.partitionGraphBy(graph, 945).cache()
          val partitionsSizes = partitionedGraph.triplets.mapPartitions(d => Iterator(d.size)).collect().toList
          val meanSize = partitionsSizes.sum / partitionsSizes.size.toDouble
          val variance = partitionsSizes.map(n => Math.pow(n - meanSize, 2)).sum / (partitionsSizes.size - 1).toDouble
          val stdDev = Math.sqrt(variance)
          Then("Should compute partitions correctly")
          logger.error(s"STDDev: ${stdDev}")
          //stdDev should be <= 10.0
        }
      }

      ignore should "Dynamic partitioning for random graph be computed in apropriate time"  taggedAs(Slow) in {
        for (x<-0 to 3) {
          logger.info(s"Run $x")
          Given("graph")
          val graph: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, 10000, 500000).cache()
          When("Partition using propagation")
          val (partitionedGraph, partitioningTime): (Graph[Int, Int], Long) = time("Partitioning")(PropagationBasedPartitioning.partitionGraphBy(graph, 24))
          Then("Should compute partitions correctly")
          partitionedGraph.edges.partitions.size should equal(24)
          partitionedGraph.triplets.partitions.size should equal(24)
          partitioningTime should be < (10000l)
          graph.unpersist(true)
          partitionedGraph.unpersist(true)
        }
  }

   ignore should "Dynamic partitioning for random graph give faster results for eignevector"  taggedAs(Slow) in{
    for (x<-0 to 3) {
      logger.info(s"Run $x")
      Given("graph")
      val graphInit: Graph[Int, Int] = GraphGenerators.rmatGraph(sc, 100000, 2050000).partitionBy(EdgePartition2D,8)
      val graph=Graph.fromEdges(graphInit.edges,0).cache()
      graph.edges.foreachPartition((_)=>{})
      graph.vertices.foreachPartition((_)=>{})
      val partitionedGraph= PropagationBasedPartitioning.partitionGraphBy(graph, 8).cache()
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

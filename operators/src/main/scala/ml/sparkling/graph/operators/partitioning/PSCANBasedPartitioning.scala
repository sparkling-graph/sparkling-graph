package ml.sparkling.graph.operators.partitioning

import java.util.UUID

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN
import ml.sparkling.graph.operators.partitioning.PropagationBasedPartitioning.logger
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * First approach to community based graph partitioning. It is not efficient due to need of gathering vertex to component id on driver node.
 */
object PSCANBasedPartitioning {

  @transient
  val logger=Logger.getLogger(PSCANBasedPartitioning.getClass())

  def partitionGraphBy[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],numberOfPartitions:Int, maxIterations:Int = Int.MaxValue)(implicit sc:SparkContext): Graph[VD, ED] ={
    val (numberOfCommunities: VertexId,  coarsedVertexMap: Map[VertexId, Int], coarsedNumberOfPartitions: Int, strategy: ByComponentIdPartitionStrategy) = buildPartitioningStrategy(graph, numberOfPartitions, maxIterations = maxIterations)
    logger.info(s"Partitioning graph using coarsed map with ${coarsedVertexMap.size} entries and ${coarsedNumberOfPartitions} partitions (before ${numberOfCommunities})")
    val out=graph.partitionBy(strategy,numberOfPartitions).cache()
    out.edges.foreachPartition((_)=>{})
    out.triplets.foreachPartition((_)=>{})
    out
  }


  def buildPartitioningStrategy[ED: ClassTag, VD: ClassTag](graph: Graph[VD, ED], numberOfPartitions: Int, maxIterations:Int = Int.MaxValue)(implicit sc:SparkContext) = {
    val (numberOfCommunities: VertexId, coarsedVertexMap: Map[VertexId, Int], coarsedNumberOfPartitions: Int) = precomputePartitions(graph, numberOfPartitions, maxIterations = maxIterations)
    logger.info(s"Requested $numberOfPartitions partitions, computed $coarsedNumberOfPartitions")
    val strategy = ByComponentIdPartitionStrategy(coarsedVertexMap, numberOfPartitions)
    (numberOfCommunities, coarsedVertexMap, coarsedNumberOfPartitions, strategy)
  }

  def precomputePartitions[ED: ClassTag, VD: ClassTag](graph: Graph[VD, ED], numberOfPartitions: Int, maxIterations:Int = Int.MaxValue)(implicit sc:SparkContext) = {
    logger.info("Computing components using PSCAN")
    val (communities, numberOfCommunities): (Graph[ComponentID, ED], VertexId) = PSCAN.computeConnectedComponentsUsing(graph, numberOfPartitions, maxIterations = maxIterations)
    val computationData=communities.vertices.map(t=>t).localCheckpoint()
    logger.info("Components computed!")
    val (coarsedVertexMap, coarsedNumberOfPartitions) = ParallelPartitioningUtils.coarsePartitions(numberOfPartitions, numberOfCommunities, computationData)
    (numberOfCommunities, coarsedVertexMap, coarsedNumberOfPartitions)
  }
}

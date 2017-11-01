package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN
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

  def partitionGraphBy[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],numberOfPartitions:Int)(implicit sc:SparkContext): Graph[VD, ED] ={
    val (numberOfCommunities: VertexId,  coarsedVertexMap: Map[VertexId, Int], coarsedNumberOfPartitions: Int, strategy: ByComponentIdPartitionStrategy) = buildPartitioningStrategy(graph, numberOfPartitions)
    logger.info(s"Partitioning graph using coarsed map with ${coarsedVertexMap.size} entries and ${coarsedNumberOfPartitions} partitions (before ${numberOfCommunities})")
    val out=new CustomGraphPartitioningImplementation[VD,ED](graph).partitionBy(strategy)
    out.edges.foreachPartition((_)=>{})
    graph.unpersist(false)
    out
  }


  def buildPartitioningStrategy[ED: ClassTag, VD: ClassTag](graph: Graph[VD, ED], numberOfPartitions: Int)(implicit sc:SparkContext) = {
    val (numberOfCommunities: VertexId, coarsedVertexMap: Map[VertexId, Int], coarsedNumberOfPartitions: Int) = precomputePartitions(graph, numberOfPartitions)
    val strategy = ByComponentIdPartitionStrategy(coarsedVertexMap, coarsedNumberOfPartitions)
    (numberOfCommunities, coarsedVertexMap, coarsedNumberOfPartitions, strategy)
  }

  def precomputePartitions[ED: ClassTag, VD: ClassTag](graph: Graph[VD, ED], numberOfPartitions: Int)(implicit sc:SparkContext) = {
    logger.info("Computing components using PSCAN")
    val (communities, numberOfCommunities): (Graph[ComponentID, ED], VertexId) = PSCAN.computeConnectedComponentsUsing(graph, numberOfPartitions)
    logger.info("Components computed!")
    val (coarsedVertexMap, coarsedNumberOfPartitions) = ParallelPartitioningUtils.coarsePartitions(numberOfPartitions, numberOfCommunities, communities.vertices)
    communities.unpersist(false)
    (numberOfCommunities, coarsedVertexMap, coarsedNumberOfPartitions)
  }
}

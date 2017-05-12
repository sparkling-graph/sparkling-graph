package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN
import ml.sparkling.graph.operators.partitioning.CommunityBasedPartitioning.{ByComponentIdPartitionStrategy, logger}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * First approach to community based graph partitioning. It is not efficient due to need of gathering vertex to component id on driver node.
 */
object PSCANBasedPartitioning {

  @transient
  val logger=Logger.getLogger(PSCANBasedPartitioning.getClass())

  def partitionGraphBy[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],numberOfPartitions:Int)(implicit sc:SparkContext): Graph[VD, ED] ={
    val communities: Graph[ComponentID, ED] = PSCAN.computeConnectedComponentsUsing(graph,numberOfPartitions)
    val numberOfCommunities=communities.vertices.values.distinct().collect().size
    val vertexToCommunityId: Map[VertexId, ComponentID] = communities.vertices.collect().toMap
    val (coarsedVertexMap,coarsedNumberOfPartitions) = PartitioningUtils.coarsePartitions(numberOfPartitions,numberOfCommunities,vertexToCommunityId)
    val strategy=ByComponentIdPartitionStrategy(coarsedVertexMap)
    logger.info(s"Partitioning graph using map with ${coarsedVertexMap.size} entries")
    graph.partitionBy(strategy,coarsedNumberOfPartitions)
  }


}

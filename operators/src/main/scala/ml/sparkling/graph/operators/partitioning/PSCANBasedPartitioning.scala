package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN
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
    logger.info("Computing components using PSCAN")
    val (communities,numberOfCommunities): (Graph[ComponentID, ED],Long) = PSCAN.computeConnectedComponentsUsing(graph,numberOfPartitions)
    logger.info("Components computed!")
    val vertexToCommunityId: Map[VertexId, ComponentID] = communities.vertices.treeAggregate(Map[VertexId,VertexId]())((agg,data)=>{agg+(data._1->data._2)},(agg1,agg2)=>agg1++agg2)
    val (coarsedVertexMap,coarsedNumberOfPartitions) = PartitioningUtils.coarsePartitions(numberOfPartitions,numberOfCommunities,vertexToCommunityId)
    val strategy=ByComponentIdPartitionStrategy(coarsedVertexMap,coarsedNumberOfPartitions)
    logger.info(s"Partitioning graph using coarsed map with ${coarsedVertexMap.size} entries (${vertexToCommunityId.size} before coarse) and ${coarsedNumberOfPartitions} partitions (before ${numberOfCommunities})")
    val out=new CustomGraphPartitioningImplementation[VD,ED](graph).partitionBy(strategy)
    out.edges.foreachPartition((_)=>{})
    graph.unpersist(false)
    out
  }


}

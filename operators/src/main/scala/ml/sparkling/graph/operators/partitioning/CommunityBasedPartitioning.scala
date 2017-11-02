package ml.sparkling.graph.operators.partitioning


import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.{CommunityDetectionAlgorithm, CommunityDetectionMethod, ComponentID}
import org.apache.log4j.Logger
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph, PartitionID, PartitionStrategy, VertexId}

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * First approach to community based graph partitioning. It is not efficient due to need of gathering vertex to component id on driver node.
 */
object CommunityBasedPartitioning {
  @transient
  val logger=Logger.getLogger(CommunityBasedPartitioning.getClass())

  def partitionGraphBy[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],communityDetectionMethod:CommunityDetectionMethod[VD,ED],numParts:Int= -1)(implicit sc:SparkContext): Graph[VD, ED] ={
    val numberOfPartitions=if (numParts== -1) sc.defaultParallelism else numParts
    val communities: Graph[ComponentID, ED] = communityDetectionMethod(graph)
    val numberOfCommunities=communities.vertices.values.mapPartitions(data=>data.toSet.iterator).treeAggregate(scala.collection.mutable.Set[VertexId]())(
      (a,b)=>a += b,
      (a,b)=>a ++= b
    ).size
    val (coarsedVertexMap,coarsedNumberOfPartitions) = ParallelPartitioningUtils.coarsePartitions(numberOfPartitions,numberOfCommunities,communities.vertices)
    val strategy=ByComponentIdPartitionStrategy(coarsedVertexMap,coarsedNumberOfPartitions)
    logger.info(s"Partitioning graph using coarsed map with ${coarsedVertexMap.size} entries  and ${coarsedNumberOfPartitions} partitions")
    val out=new CustomGraphPartitioningImplementation[VD,ED](graph).partitionBy(strategy).cache()
    out.edges.foreachPartition((_)=>{})
    out.vertices.foreachPartition((_)=>{})
    out
  }


  def partitionGraphUsing[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],communityDetectionMethod:CommunityDetectionAlgorithm,numParts:Int= -1)(implicit sc:SparkContext): Graph[VD, ED] ={
    partitionGraphBy(graph,communityDetectionMethod.detectCommunities[VD,ED](_),numParts)
  }



}

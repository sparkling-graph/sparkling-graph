package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.{CommunityDetectionAlgorithm, CommunityDetectionMethod, ComponentID}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph, PartitionID, PartitionStrategy, VertexId}

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * First approach to community based graph partitioning. It is not efficient due to need of gathering vertex to component id on driver node.
 */
object CommunityBasedPartitioning {


  def partitionGraphBy[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],communityDetectionMethod:CommunityDetectionMethod[VD,ED])(implicit sc:SparkContext): Graph[VD, ED] ={
    val communities: Graph[ComponentID, ED] = communityDetectionMethod(graph)
    val numberOfCommunities=communities.vertices.values.distinct().collect().size
    val vertexToCommunityId: Map[VertexId, ComponentID] = communities.vertices.collect().toMap
    val broadcastedMap =sc.broadcast(vertexToCommunityId)
    val strategy=ByComponentIdPartitionStrategy(broadcastedMap)
    graph.partitionBy(strategy,numberOfCommunities)
  }


  def partitionGraphBy[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],communityDetectionMethod:CommunityDetectionAlgorithm)(implicit sc:SparkContext): Graph[VD, ED] ={
    partitionGraphBy(graph,communityDetectionMethod.detectCommunities[VD,ED](_))
  }


   case class ByComponentIdPartitionStrategy(idMap:Broadcast[Map[VertexId, ComponentID]]) extends PartitionStrategy{
    override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
      val vertex1Component: ComponentID = idMap.value(src)
      val vertex2Component: ComponentID = idMap.value(dst)
      Math.min(vertex1Component,vertex2Component).toInt
    }
  }
}

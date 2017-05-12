package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import org.apache.spark.graphx.{PartitionID, VertexId}

import scala.collection.immutable


/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 12.05.17.
  */
object PartitioningUtils {
  def coarsePartitions(numberOfPartitions: PartitionID, numberOfCommunities: VertexId, vertexToCommunityId: Map[VertexId, ComponentID]) = {
    if (numberOfCommunities > numberOfPartitions) {
      var communities= vertexToCommunityId.toList.map(t => (t._2, t._1)).groupBy(t => t._1).toSeq.sortBy(_._2.length);
      while (communities.length > numberOfPartitions && communities.length >= 2) {
        communities= communities match{
          case (firstCommunityId,firstData)::(secondCommunityId,secondData)::tail=>((Math.min(firstCommunityId,secondCommunityId),firstData:::secondData)::tail).sortBy(_._2.length)
          case t::Nil=>t::Nil
          case _ => Nil
        }
      }
      (communities.flatMap {
        case (community: ComponentID, data: immutable.Seq[(ComponentID, VertexId)]) => data.map {
          case (_, id) => (id, community)
        }
      }.toMap, communities.length)
    } else {
      (vertexToCommunityId, numberOfCommunities.toInt)
    }
  }
}

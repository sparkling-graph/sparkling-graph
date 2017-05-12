package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import org.apache.spark.graphx.{PartitionID, VertexId}


/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 12.05.17.
  */
object PartitioningUtils {
  def coarsePartitions(numberOfPartitions: PartitionID, numberOfCommunities: VertexId, vertexToCommunityId: Map[VertexId, ComponentID]) = {
    if (numberOfCommunities > numberOfPartitions) {
      var communities = vertexToCommunityId.map(t => (t._2, t._1)).toList.groupBy(t => t._1).toList.sortBy(_._2.length);
      while (communities.length > numberOfPartitions && communities.length >= 2) {
        communities = ((communities.head._1, communities.head._2 ::: communities.tail.head._2) :: communities.tail.tail).sortBy(_._2.length);
      }
      (communities.flatMap {
        case (community, data) => data.map {
          case (_, id) => (id, community)
        }
      }.toMap, communities.length)
    } else {
      (vertexToCommunityId, numberOfCommunities.toInt)
    }
  }
}

package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import org.apache.log4j.Logger
import org.apache.spark.graphx.{PartitionID, VertexId}

import scala.collection.immutable


/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 12.05.17.
  */
object PartitioningUtils {

  @transient
  val logger=Logger.getLogger(PartitioningUtils.getClass())

  def coarsePartitions(numberOfPartitions: PartitionID, numberOfCommunities: VertexId, vertexToCommunityId: Map[VertexId, ComponentID]) = {
    if (numberOfCommunities > numberOfPartitions) {
      logger.info(s"Number of communities ($numberOfCommunities) is bigger thant requested number of partitions ($numberOfPartitions)")
      var communities= vertexToCommunityId.toList.map(t => (t._2, t._1)).groupBy(t => t._1).toList.sortBy(_._2.length);
      while (communities.length > numberOfPartitions && communities.length >= 2) {
        logger.info(s"Coarsing two smallest communities into one community, size before coarse: ${communities.length}")
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
      }.toMap, communities.map(_._1).toSet.size)
    } else {
      logger.info(s"Number of communities ($numberOfCommunities) is not bigger thant requested number of partitions ($numberOfPartitions)")
      (vertexToCommunityId, numberOfCommunities.toInt)
    }
  }
}

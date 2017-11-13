package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import org.apache.log4j.Logger
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * Created by  Roman Bartusiak <riomus@gmail.com> on 12.05.17.
 */
object ParallelPartitioningUtils {

  @transient
  val logger = Logger.getLogger(ParallelPartitioningUtils.getClass())

  def coarsePartitions(numberOfPartitions: PartitionID, numberOfCommunities: Long, vertexToCommunityId: RDD[(VertexId, ComponentID)], parallelLimit: Long = 50000, givenPartitions: Int = -1): (Map[VertexId, Int], Int) = {
    val partitions = if (givenPartitions < 1) {
      vertexToCommunityId.sparkContext.defaultParallelism
    } else {
      givenPartitions
    }
    val (map, size) = if (numberOfCommunities > numberOfPartitions) {
      logger.info(s"Number of communities ($numberOfCommunities) is bigger thant requested number of partitions ($numberOfPartitions), using $partitions partitions")
      var communities = vertexToCommunityId.map(t => (t._2, t._1)).aggregateByKey(List[VertexId](), partitions)(
        (buff, id) => {
          id :: buff
        },
        (buff1, buff2) => {
          buff1 ::: buff2
        }
      ).repartition(partitions).sortBy(_._2.length)
      var communitiesCount = communities.count()
      var oldCommunitiesCount = -1l
      while (communitiesCount > numberOfPartitions && communitiesCount >= 2 && communitiesCount > parallelLimit && oldCommunitiesCount != communitiesCount) {
        val toReduce = communitiesCount - numberOfPartitions
        logger.info(s"Coarsing smallest communities into one community, size before coarse: ${communitiesCount}, need to coarse $toReduce")
        val newCommunities = communities.mapPartitionsWithIndex(
          (id, data) => {
            if (id == 0) {
              var reduced = 0
              val localData = ListBuffer(data.toSeq:_*);
              val maxSize = localData.last._2.length
              var continue = true
              var lastAddedSize = -1
              var lastAddedIndex = -1
              while (reduced < toReduce && continue && localData.length > 2) {
                val (fId, fData) = localData(0)
                val (sId, sData) = localData(1)
                continue = fData.length <= maxSize && sData.length <= maxSize
                if (continue) {
                  localData.remove(0)
                  localData.remove(0)
                  val data = fData:::sData
                  val entity = (Math.min(fId, sId), data)
                  val entityLength = data.length
                  val i = if (entityLength == lastAddedSize) {
                    lastAddedIndex = Math.max(lastAddedIndex - 1, 0)
                    lastAddedIndex
                  } else {
                    val i = localData.toStream.zipWithIndex.find {
                      case ((_, list), _) => list.length >= entityLength
                    }.map {
                      case ((_, _), index) => index
                    }.getOrElse(localData.length)
                    lastAddedSize = entityLength
                    lastAddedIndex = i
                    i
                  }
                  localData.insert(i,entity)
                  reduced += 1
                }
              }
              localData.toIterator
            } else {
              data
            }
          }, true).repartition(partitions).sortBy(_._2.length).cache()
        communities = newCommunities
        oldCommunitiesCount = communitiesCount
        communitiesCount = communities.count()
        logger.info(s"Coarsed communities: $communitiesCount , from $oldCommunitiesCount")
      }
      val outMap = communities.flatMap {
        case (community, data) => data.map((id) => (id, community))
      }
      (outMap, communitiesCount.toInt)
    } else {
      logger.info(s"Number of communities ($numberOfCommunities) is not bigger thant requested number of partitions ($numberOfPartitions)")
      (vertexToCommunityId, numberOfCommunities.toInt)
    }
    val componentsIds = map.map(_._2).distinct.zipWithIndex()
    val outMap = map.map {
      case (vId, cId) => (cId, vId)
    }.join(componentsIds).map(_._2).treeAggregate(mutable.Map.empty[VertexId, ComponentID])(
      (buff, t) => {
        buff += ((t._1, t._2.toInt)); buff
      },
      (buff1, buff2) => {
        buff1 ++= buff2;
        buff1
      }
    ).toMap
    PartitioningUtils.coarsePartitions(numberOfPartitions, size, outMap)
  }
}

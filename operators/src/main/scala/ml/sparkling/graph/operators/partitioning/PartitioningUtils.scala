package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import ml.sparkling.graph.operators.utils.LoggerHolder
import org.apache.log4j.Logger
import org.apache.spark.Partitioner
import org.apache.spark.graphx._

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag


/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 12.05.17.
  */
object PartitioningUtils {

  @transient
  val logger=Logger.getLogger(PartitioningUtils.getClass())

  def coarsePartitions(numberOfPartitions: PartitionID, numberOfCommunities: Long, vertexToCommunityId: Map[VertexId, ComponentID]): (Map[VertexId, Int], Int) = {
    val (map,size)=if (numberOfCommunities > numberOfPartitions) {
      logger.info(s"Number of communities ($numberOfCommunities) is bigger thant requested number of partitions ($numberOfPartitions)")
      val communities= ListBuffer(vertexToCommunityId.toList.map(t => (t._2, t._1)).groupBy(t => t._1).toList
        .map{ case (id,data)=>(id,ListBuffer(data:_*))}.sortBy(_._2.length):_*);
      var lastAddedSize= -1
      var lastAddedIndex= -1
      while (communities.length > numberOfPartitions && communities.length >= 2) {
        logger.debug(s"Coarsing two smallest communities into one community, size before coarse: ${communities.length}")
            val (firstCommunityId,firstData)=communities(0)
            val (secondCommunityId,secondData)=communities(1)
            communities.remove(0)
            communities.remove(0)
            firstData++=secondData
            val entity=(Math.min(firstCommunityId,secondCommunityId),firstData)
            val entityLength=firstData.length
            val i = if(entityLength==lastAddedSize){
              lastAddedIndex=Math.max(lastAddedIndex-1,0)
              lastAddedIndex
            }else{
              val i = communities.toStream.zipWithIndex.find {
                case ((_, list), _) => list.length >= entityLength
              }.map {
                case ((_, _), index) => index
              }.getOrElse(communities.length)
              lastAddedSize=entityLength
              lastAddedIndex=i
              i
            }
            communities.insert(i,entity)
        }
      (communities.flatMap {
        case (community, data) => data.map {
          case (_, id) => (id, community)
        }
      }.toMap, communities.map(_._1).toSet.size)
    } else {
      logger.info(s"Number of communities ($numberOfCommunities) is not bigger thant requested number of partitions ($numberOfPartitions)")
      (vertexToCommunityId, numberOfCommunities.toInt)
    }
    val componentsIds: List[ComponentID] = map.values.toList.distinct
    val componentMapper: Map[ComponentID, Int] = componentsIds.zipWithIndex.toMap
    val outMap: Map[VertexId, Int] =map.mapValues(componentMapper).map(identity)
    (outMap,size)
  }
}


class CustomGraphPartitioningImplementation[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED]){
  def partitionBy(partitionStrategy: ByComponentIdPartitionStrategy): Graph[VD, ED] = {
    graph.partitionBy(partitionStrategy,partitionStrategy.partitions)
  }
}

case class ByComponentIdPartitionStrategy(idMap:Map[VertexId, Int],partitions:Int) extends Partitioner  with  PartitionStrategy{

  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    val logger=LoggerHolder.log
    val vertex1Component: Int = idMap(src)
    val vertex2Component: Int = idMap(dst)
    val partition=Math.min(vertex1Component,vertex2Component)
    logger.debug(s"Partitioning edge $src($vertex1Component) - $dst($vertex2Component) to $partition")
    partition
  }

  override def numPartitions: PartitionID = partitions

  override def getPartition(key: Any): PartitionID = {
    key match{
      case key:VertexId=>{val component: Int=idMap.getOrElse(key,Int.MaxValue);component}
      case  k => throw new RuntimeException(s"Unable to get partition for $k")
    }

  }
}
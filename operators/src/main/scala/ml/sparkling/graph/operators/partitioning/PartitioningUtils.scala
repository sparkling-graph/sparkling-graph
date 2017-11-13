package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import org.apache.log4j.Logger
import org.apache.spark.{ Partitioner}
import org.apache.spark.graphx._

import scala.collection.immutable
import scala.reflect.{ClassTag}


/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 12.05.17.
  */
object PartitioningUtils {

  @transient
  val logger=Logger.getLogger(PartitioningUtils.getClass())

  def coarsePartitions(numberOfPartitions: PartitionID, numberOfCommunities: Long, vertexToCommunityId: Map[VertexId, ComponentID]): (Map[VertexId, Int], Int) = {
    val (map,size)=if (numberOfCommunities > numberOfPartitions) {
      logger.info(s"Number of communities ($numberOfCommunities) is bigger thant requested number of partitions ($numberOfPartitions)")
      var communities= vertexToCommunityId.toList.map(t => (t._2, t._1)).groupBy(t => t._1).toList.sortBy(_._2.length);
      while (communities.length > numberOfPartitions && communities.length >= 2) {
        logger.debug(s"Coarsing two smallest communities into one community, size before coarse: ${communities.length}")
        communities= communities match{
          case (firstCommunityId,firstData)::(secondCommunityId,secondData)::tail=>{
            val entity=(Math.min(firstCommunityId,secondCommunityId),firstData:::secondData)
            val entityLength=entity._2.length
            val i=tail.toStream.zipWithIndex.find{
              case ((_,list),_)=>list.length>=entityLength
            }.map{
              case ((_,_),index)=>index
            }.getOrElse(tail.length)
            val (before,after)=tail.splitAt(i)
            before ::: (entity :: after)
          }
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
    val componentsIds: List[ComponentID] =map.values.toSet.toList
    val componentMapper: Map[ComponentID, Int] =componentsIds.zipWithIndex.toMap
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
    val vertex1Component: Int = idMap.getOrElse(src,Int.MaxValue)
    val vertex2Component: Int = idMap.getOrElse(dst,Int.MaxValue)
    Math.min(vertex1Component,vertex2Component)
  }

  override def numPartitions: PartitionID = partitions

  override def getPartition(key: Any): PartitionID = {
    key match{
      case key:VertexId=>{val component: Int=idMap.getOrElse(key,Int.MaxValue);component}
      case  k => throw new RuntimeException(s"Unable to get partition for $k")
    }

  }
}
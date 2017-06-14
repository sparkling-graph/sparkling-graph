package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection
import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Partitioner
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.{immutable, mutable}
import scala.reflect.ClassTag


/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 12.05.17.
  */
object PartitioningUtils {

  @transient
  val logger=Logger.getLogger(PartitioningUtils.getClass())

  def coarsePartitions(numberOfPartitions: PartitionID, numberOfCommunities: Long, vertexToCommunityId: RDD[(VertexId, ComponentID)]): (Map[VertexId, Int], Int) = {
    val (map,size)=if (numberOfCommunities > numberOfPartitions) {
      logger.info(s"Number of communities ($numberOfCommunities) is bigger thant requested number of partitions ($numberOfPartitions)")
      var communities: Seq[(CommunityDetection.ComponentID, List[VertexId])] = vertexToCommunityId.aggregateByKey(mutable.ListBuffer.empty[VertexId])(
        (buff,id)=>{buff+=id;buff},
        (buff1,buff2)=>{buff1 ++= buff2;buff1}
      ).sortBy(_._2.length).collect().map{
        case (id,data)=>(id,data.toList)
      }.toList
      while (communities.length > numberOfPartitions && communities.length >= 2) {
        logger.debug(s"Coarsing two smallest communities into one community, size before coarse: ${communities.length}")
        communities= communities match{
          case (firstCommunityId,firstData)::(secondCommunityId,secondData)::tail=>{
            val newData=firstData:::secondData
            val newTuple=(Math.min(firstCommunityId,secondCommunityId),newData)
            val splitIndex=tail.indexWhere{
              case (_,data)=>data.size>newData.size
            }
            if(splitIndex>0){
              val (front,newTail)=tail.splitAt(splitIndex)
              front ::: newTuple :: newTail
            }else{
              newTuple::tail
            }
          }
          case t::Nil=>t::Nil
          case _ => Nil
        }
      }
      (communities.flatMap {
        case (community: ComponentID, data: immutable.Seq[VertexId]) => data.map {
          id => (id, community)
        }
      }.toMap, communities.map(_._1).toSet.size)
    } else {
      logger.info(s"Number of communities ($numberOfCommunities) is not bigger thant requested number of partitions ($numberOfPartitions)")
      (vertexToCommunityId.treeAggregate(mutable.Map.empty[VertexId,ComponentID])(
        (buff,t)=>{buff+=((t._1,t._2.toInt));buff},
        (buff1,buff2)=>{
          buff1++=buff2;
          buff1
        }
      ).toMap, numberOfCommunities.toInt)
    }
    val componentsIds: List[ComponentID] =map.values.toSet.toList
    val componentMapper: Map[ComponentID, Int] =componentsIds.zipWithIndex.toMap
    val outMap: Map[VertexId, Int] =map.mapValues(componentMapper).map(identity)
    (outMap,size)
  }
}


class CustomGraphPartitioningImplementation[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED]){
  def partitionBy(partitionStrategy: ByComponentIdPartitionStrategy): Graph[VD, ED] = {
    val prePartitionedGraph=graph.partitionBy(partitionStrategy,partitionStrategy.partitions)
    val partitionedEdges=prePartitionedGraph.edges;
    val partitionedVertices=prePartitionedGraph.vertices.partitionBy(partitionStrategy)
    val out=Graph.fromEdges(partitionedEdges,null.asInstanceOf[VD])
    prePartitionedGraph.unpersist(false)
    partitionedEdges.unpersist(false)
    partitionedVertices.unpersist(false)
    out
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
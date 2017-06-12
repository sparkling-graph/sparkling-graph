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

  def coarsePartitions(numberOfPartitions: PartitionID, numberOfCommunities: VertexId, vertexToCommunityId: RDD[(VertexId, ComponentID)]):(Map[VertexId, Int], Int) = {
    val (map,size)=if (numberOfCommunities > numberOfPartitions) {
      logger.info(s"Number of communities ($numberOfCommunities) is bigger thant requested number of partitions ($numberOfPartitions)")
      var communities= vertexToCommunityId.map(t => (t._2, t._1)).aggregateByKey(mutable.ListBuffer.empty[VertexId])(
        (buff,id)=>{buff+=id;buff},
        (buff1,buff2)=>{buff1 ++= buff2;buff1}
      ).sortBy(_._2.length)
      while (communities.count() > numberOfPartitions && communities.count()  >= 2) {
        logger.debug(s"Coarsing two smallest communities into one community, size before coarse: ${communities.count()}")
        val newCommunities= communities.mapPartitionsWithIndex{
          case (id,data)=>{
            if(id==0){
              data.toList match {
                case (fId,fData)::(sId,sData)::tail=>{
                  fData++=sData
                  (Math.min(fId,sId),fData)::tail
                }.toIterator
                case _ => data
              }
            }else{
              data
            }
          }
        }.sortBy(_._2.length)
        communities.unpersist()
        communities=newCommunities;
      }
      val outMap=communities.flatMap{
        case (community, data) => data.map((id) => (id, community))
      }
      (outMap, communities.count().toInt)
    } else {
      logger.info(s"Number of communities ($numberOfCommunities) is not bigger thant requested number of partitions ($numberOfPartitions)")
      (vertexToCommunityId, numberOfCommunities.toInt)
    }
    val componentsIds =map.map(_._2).distinct.zipWithIndex()
    val outMap=map.map{
      case (vId,cId)=>(cId,vId)
    }.join(componentsIds).map(_._2).treeAggregate(mutable.Map.empty[VertexId,Int])(
      (buff,t)=>{buff+=((t._1,t._2.toInt));buff},
      (buff1,buff2)=>{
        buff1++=buff2;
        buff1
      }
    ).toMap
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
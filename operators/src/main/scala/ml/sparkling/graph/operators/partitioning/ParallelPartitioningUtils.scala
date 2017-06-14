package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import org.apache.log4j.Logger
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable


/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 12.05.17.
  */
object ParallelPartitioningUtils {

  @transient
  val logger=Logger.getLogger(ParallelPartitioningUtils.getClass())

  def coarsePartitions(numberOfPartitions: PartitionID, numberOfCommunities: Long, vertexToCommunityId: RDD[(VertexId, ComponentID)],parallelLimit:Long=50000,partitions:Int=15):(Map[VertexId, Int], Int) = {
    val (map,size)=if (numberOfCommunities > numberOfPartitions) {
      logger.info(s"Number of communities ($numberOfCommunities) is bigger thant requested number of partitions ($numberOfPartitions)")
      var communities= vertexToCommunityId.map(t => (t._2, t._1)).aggregateByKey(mutable.ListBuffer.empty[VertexId])(
        (buff,id)=>{buff+=id;buff},
        (buff1,buff2)=>{buff1 ++= buff2;buff1}
      ).sortBy(_._2.length)
      while (communities.count() > numberOfPartitions && communities.count()  >= 2 && communities.count()>parallelLimit) {
        val toReduce=communities.count() - numberOfPartitions
        logger.info(s"Coarsing two smallest communities into one community, size before coarse: ${communities.count()}, need to coarse $toReduce")
        val newCommunities= communities.mapPartitionsWithIndex{
          case (id,data)=>{
            if(id==0){
              var reduced=0
              var continue=true
              var localData=data.toList;
              val maxSize=localData.last._2.length
              while (reduced<toReduce&&continue){
                localData match {
                case (fId,fData)::(sId,sData)::tail=>{
                  continue=fData.length<=maxSize&&sData.length<=maxSize
                  if(continue){
                    fData++=sData
                    localData=((Math.min(fId,sId),fData)::tail).sortBy(_._2.length)
                    reduced += 1
                  }
                }
                case _ => {continue=false}
              }
            }
              localData.toIterator
            }else{
              data
            }
          }
        }.sortBy(_._2.length,numPartitions = partitions).cache()
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
    }.join(componentsIds).map(_._2).treeAggregate(mutable.Map.empty[VertexId,ComponentID])(
      (buff,t)=>{buff+=((t._1,t._2.toInt));buff},
      (buff1,buff2)=>{
        buff1++=buff2;
        buff1
      }
    ).toMap
    PartitioningUtils.coarsePartitions(numberOfPartitions,size,outMap)
  }
}

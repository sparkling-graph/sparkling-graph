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

  def coarsePartitions(numberOfPartitions: PartitionID, numberOfCommunities: Long, vertexToCommunityId: RDD[(VertexId, ComponentID)],parallelLimit:Long=50000,givenPartitions:Int= -1):(Map[VertexId, Int], Int) = {
    val partitions= if(givenPartitions<1){vertexToCommunityId.partitions.length} else {givenPartitions}
    val (map,size)=if (numberOfCommunities > numberOfPartitions) {
      logger.info(s"Number of communities ($numberOfCommunities) is bigger thant requested number of partitions ($numberOfPartitions)")
      var communities= vertexToCommunityId.map(t => (t._2, t._1)).aggregateByKey(List[VertexId](),partitions)(
        (buff,id)=> id :: buff ,
        (buff1,buff2)=>buff1:::buff2
      ).sortBy(_._2.length,numPartitions = partitions)
      var communitiesCount=communities.count()
      var oldCommunitiesCount = -1l
      while (communitiesCount > numberOfPartitions &&communitiesCount  >= 2 && communitiesCount>parallelLimit && oldCommunitiesCount != communitiesCount) {
        val toReduce=communitiesCount - numberOfPartitions
        logger.info(s"Coarsing two smallest communities into one community, size before coarse: ${communitiesCount}, need to coarse $toReduce")
        val newCommunities= communities.mapPartitionsWithIndex(
         (id, data) => {
              if (id == 0) {
                var reduced = 0
                var localData = data.toList;
                var continue = true
                val maxSize = localData.last._2.length
                var lastAddedSize= -1
                var lastAddedIndex= -1
                while (reduced < toReduce && continue) {
                  localData match {
                    case (fId, fData) :: (sId, sData) :: tail => {
                      continue = fData.length <= maxSize && sData.length <= maxSize
                      if (continue) {
                        val data=fData:::sData
                        val entity = (Math.min(fId, sId), data)
                        val entityLength = data.length
                        val i = if(entityLength==lastAddedSize){
                          lastAddedIndex=Math.max(lastAddedIndex-1,0)
                          lastAddedIndex
                        }else{
                          val i = tail.toStream.zipWithIndex.find {
                            case ((_, list), _) => list.length >= entityLength
                          }.map {
                            case ((_, _), index) => index
                          }.getOrElse(tail.length)
                          lastAddedSize=entityLength
                          lastAddedIndex=i
                          i
                        }
                        val (before, after) = tail.splitAt(i)
                        localData = before ::: (entity :: after)
                        reduced += 1
                      }
                    }
                    case _ => {
                      continue = false
                    }
                  }
                }
                localData.toIterator
              } else {
                data
              }
            },true).sortBy(_._2.length,numPartitions = partitions).cache()
        communities=newCommunities
        oldCommunitiesCount=communitiesCount
        communitiesCount=communities.count()
        logger.info(s"Coarsed communities: $communitiesCount , from $oldCommunitiesCount")
      }
      val outMap=communities.flatMap{
        case (community, data) => data.map((id) => (id, community))
      }
      (outMap, communitiesCount.toInt)
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

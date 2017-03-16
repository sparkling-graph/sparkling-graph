package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.{CommunityDetectionAlgorithm, CommunityDetectionMethod, ComponentID}
import ml.sparkling.graph.operators.partitioning.CommunityBasedPartitioning.ByComponentIdPartitionStrategy
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph, PartitionID, PartitionStrategy, VertexId}

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * First approach to community based graph partitioning. It is not efficient due to need of gathering vertex to component id on driver node.
 */
object PropagationBasedPartitioning {

  val logger=Logger.getLogger(PropagationBasedPartitioning.getClass())

  def partitionGraphBy[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],numParts:Int= -1,checkpointingFrequency:Int=10)(implicit sc:SparkContext): Graph[VD, ED] ={
    val numberOfPartitions=if (numParts== -1) sc.defaultParallelism else numParts

    var operationGraph=graph.mapVertices{
      case (vId,_)=>vId
    }
    var oldComponents=operationGraph.vertices;

    var numberOfComponents=graph.numVertices;
    var oldNumberOfComponents=Long.MaxValue;
    var iteration=0;
    while ((numberOfComponents>numberOfPartitions && numberOfComponents!=1 && oldNumberOfComponents!=numberOfComponents) || oldNumberOfComponents>Int.MaxValue){
      logger.info(s"Propagation based partitioning: iteration:$iteration, last number of components:$oldNumberOfComponents, current number of components:$numberOfComponents")
      iteration=iteration+1;
      oldComponents=operationGraph.vertices.cache();
      val newIds=operationGraph.aggregateMessages[VertexId](ctx=>{
        if(ctx.srcAttr<ctx.dstAttr){
          ctx.sendToDst(ctx.srcAttr)
        }else if(ctx.dstAttr<ctx.srcAttr){
          ctx.sendToSrc(ctx.dstAttr)
        }
      },Math.min).cache()
      operationGraph=operationGraph.outerJoinVertices(newIds){
        case (vId,oldData,newData)=>newData.getOrElse(oldData)
      }.cache()
      oldNumberOfComponents=numberOfComponents
      numberOfComponents=operationGraph.vertices.map(_._2).distinct().count()
      if(iteration%checkpointingFrequency==0){
        oldComponents.checkpoint();
        operationGraph.checkpoint();
        operationGraph.vertices.foreachPartition((_)=>{})
        operationGraph.edges.foreachPartition((_)=>{})
        oldComponents.foreachPartition((_)=>{})
      }
    }

    val (communities,numberOfCommunities)=(oldComponents,oldNumberOfComponents)
    val vertexToCommunityId: Map[VertexId, ComponentID] = communities.treeAggregate(Map[VertexId,VertexId]())((agg,data)=>{agg+(data._1->data._2)},(agg1,agg2)=>agg1++agg2)
    val broadcastedMap =sc.broadcast(vertexToCommunityId)
    val strategy=ByComponentIdPartitionStrategy(broadcastedMap)
    graph.partitionBy(strategy,numberOfCommunities.toInt)
  }

}

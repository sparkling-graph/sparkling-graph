package ml.sparkling.graph.operators.partitioning

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * First approach to community based graph partitioning. It is not efficient due to need of gathering vertex to component id on driver node.
 */
object PropagationBasedPartitioning {

  val logger=Logger.getLogger(PropagationBasedPartitioning.getClass())

  def precomputePartitions[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED], numParts:Int, checkpointingFrequency:Int=50,
                                                    vertexOperator: VertexOperator)(implicit sc:SparkContext):(Map[VertexId, Int], Int)={
    var operationGraph=graph.mapVertices{
      case (vId,_)=>vId
    }.cache()
    var oldComponents: VertexRDD[VertexId] =operationGraph.vertices;

    var numberOfComponents=graph.numVertices;
    var oldNumberOfComponents=Long.MaxValue;
    var iteration=0;
    while ((numberOfComponents>numParts && numberOfComponents!=1 && oldNumberOfComponents!=numberOfComponents) || oldNumberOfComponents>Int.MaxValue){
      logger.info(s"Propagation based partitioning: iteration:$iteration, last number of components:$oldNumberOfComponents, current number of components:$numberOfComponents")
      iteration=iteration+1;
      oldComponents=operationGraph.vertices
      val newIds=operationGraph.aggregateMessages[VertexId](ctx=>{
        val vertex = vertexOperator(ctx.srcAttr, ctx.dstAttr)
        if(ctx.srcAttr == vertex){
          ctx.sendToDst(ctx.srcAttr)
        }else if(ctx.dstAttr == vertex) {
          ctx.sendToSrc(ctx.dstAttr)
        }
      },vertexOperator.apply _)

      val newOperationGraph=operationGraph.joinVertices(newIds)((_,_,newData)=>newData).cache()
      operationGraph=newOperationGraph
      oldNumberOfComponents=numberOfComponents
      numberOfComponents=operationGraph.vertices.map(_._2).countApproxDistinct()
      if(iteration%checkpointingFrequency==0){
        oldComponents.checkpoint();
        operationGraph.checkpoint();
        operationGraph.vertices.foreachPartition((_)=>{})
        operationGraph.edges.foreachPartition((_)=>{})
        oldComponents.foreachPartition((_)=>{})
      }
    }
    val (communities,numberOfCommunities)=(oldComponents,oldNumberOfComponents)
    return ParallelPartitioningUtils.coarsePartitions(numParts, numberOfCommunities, communities)
  }

  def partitionGraphBy[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],numParts:Int= -1,checkpointingFrequency:Int=50,
                                                vertexOperator: VertexOperator = DefaultVertexOperator,
                                                partitionOperator: VertexOperator = DefaultPartitionOperator)(implicit sc:SparkContext): Graph[VD, ED] ={
    val numberOfPartitions=if (numParts== -1) sc.defaultParallelism else numParts
    val (vertexMap: Map[VertexId, Int], newNumberOfCommunities: Int, strategy: ByComponentIdPartitionStrategy) =
      buildPartitioningStrategy(graph, numberOfPartitions, checkpointingFrequency, partitionOperator)
    logger.info(s"Partitioning graph using coarsed map with ${vertexMap.size} entries and ${newNumberOfCommunities} partitions")
    val out=graph.partitionBy(strategy,numberOfPartitions).cache()
    out.edges.foreachPartition((_)=>{})
    out.vertices.foreachPartition((_)=>{})
    out.triplets.foreachPartition((_)=>{})
    out
  }

  def buildPartitioningStrategy[ED: ClassTag, VD: ClassTag](graph: Graph[VD, ED], numParts: Int,
                                                            checkpointingFrequency: Int,
                                                            vertexOperator: VertexOperator)(implicit sc:SparkContext) = {
    val (vertexMap, newNumberOfCommunities) = precomputePartitions(graph, numParts, checkpointingFrequency, vertexOperator);
    logger.info(s"Requested $numParts partitions, computed $newNumberOfCommunities")
    val strategy = ByComponentIdPartitionStrategy(vertexMap, numParts, vertexOperator)
    (vertexMap, newNumberOfCommunities, strategy)
  }

  trait VertexOperator extends Serializable{
    def apply(v1: VertexId, v2:VertexId): VertexId
  }
  object DefaultVertexOperator extends VertexOperator{
    def apply(v1: VertexId, v2: VertexId): VertexId = {
      math.min(v1, v2)
    }
  }
  object DefaultPartitionOperator extends VertexOperator{
    def apply(v1: VertexId, v2: VertexId): VertexId = {
      math.max(v1, v2)
    }
  }
}

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

  def precomputePartitions[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],numParts:Int= -1,checkpointingFrequency:Int=50)(implicit sc:SparkContext):(Map[VertexId, Int], Int)={
    val numberOfPartitions=if (numParts== -1) sc.defaultParallelism else numParts
    var operationGraph=graph.mapVertices{
      case (vId,_)=>vId
    }.cache()
    var oldComponents: VertexRDD[VertexId] =operationGraph.vertices;

    var numberOfComponents=graph.numVertices;
    var oldNumberOfComponents=Long.MaxValue;
    var iteration=0;
    while ((numberOfComponents>numberOfPartitions && numberOfComponents!=1 && oldNumberOfComponents!=numberOfComponents) || oldNumberOfComponents>Int.MaxValue){
      logger.info(s"Propagation based partitioning: iteration:$iteration, last number of components:$oldNumberOfComponents, current number of components:$numberOfComponents")
      iteration=iteration+1;
      oldComponents=operationGraph.vertices
      val newIds=operationGraph.aggregateMessages[VertexId](ctx=>{
        if(ctx.srcAttr<ctx.dstAttr){
          ctx.sendToDst(ctx.srcAttr)
          ctx.sendToSrc(ctx.srcAttr)
        }else {
          ctx.sendToSrc(ctx.dstAttr)
          ctx.sendToDst(ctx.dstAttr)
        }
      },math.min)

      val newOperationGraph=Graph(newIds,operationGraph.edges).cache()
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
    return ParallelPartitioningUtils.coarsePartitions(numberOfPartitions, numberOfCommunities, communities)
  }

  def partitionGraphBy[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],numParts:Int= -1,checkpointingFrequency:Int=50)(implicit sc:SparkContext): Graph[VD, ED] ={
    val (vertexMap: Map[VertexId, Int], newNumberOfCummunities: Int, strategy: ByComponentIdPartitionStrategy) = buildPartitioningStrategy(graph, numParts, checkpointingFrequency)
    logger.info(s"Partitioning graph using coarsed map with ${vertexMap.size} entries and ${newNumberOfCummunities} partitions")
    val out=new CustomGraphPartitioningImplementation[VD,ED](graph).partitionBy(strategy).cache()
    out.edges.foreachPartition((_)=>{})
    out.vertices.foreachPartition((_)=>{})
    out
  }

  def buildPartitioningStrategy[ED: ClassTag, VD: ClassTag](graph: Graph[VD, ED], numParts: Int, checkpointingFrequency: Int)(implicit sc:SparkContext) = {
    val (vertexMap, newNumberOfCummunities) = precomputePartitions(graph, numParts, checkpointingFrequency);
    val strategy = ByComponentIdPartitionStrategy(vertexMap, newNumberOfCummunities)
    (vertexMap, newNumberOfCummunities, strategy)
  }
}

package ml.sparkling.graph.operators.algorithms.aproximation

import java.util

import ml.sparkling.graph.api.operators.IterativeComputation._
import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.Component
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes.{JDouble, JLong, JMap}
import ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation.LPCoarsening
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.{PathProcessor, SingleVertexProcessor}
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.{FastUtilWithDistance, FastUtilWithPath}
import ml.sparkling.graph.operators.predicates.{AllPathPredicate, ByIdPredicate, ByIdsPredicate}
import org.apache.spark.graphx.{EdgeTriplet, Graph, _}
import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance.DataMap
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 07.02.17.
  */
case object ApproximatedShortestPathsAlgorithm  {
  val logger=Logger.getLogger(ApproximatedShortestPathsAlgorithm.getClass())

  type PathModifier=(VertexId,VertexId,JDouble)=>JDouble

  val defaultNewPath:(JDouble=>JDouble)= (path:JDouble)=>3*path+2;
  val defaultPathModifier:PathModifier= (fromVertex:VertexId, toVertex:VertexId, path:JDouble)=>defaultNewPath(path)

  def computeShortestPathsLengthsUsing[VD:ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPredicate: SimpleVertexPredicate= AllPathPredicate, treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[DataMap,ED] = {
    val coarsedGraph=LPCoarsening.coarse(graph,treatAsUndirected)
    computeShortestPathsLengthsWithoutCoarsingUsing(graph,coarsedGraph,vertexPredicate,treatAsUndirected,modifier)
  }

  def computeShortestPathsLengthsWithoutCoarsingUsing[VD:ClassTag, ED: ClassTag](graph: Graph[VD, ED], coarsedGraph: Graph[Component, ED], vertexPredicate: SimpleVertexPredicate= AllPathPredicate, treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[DataMap,ED] = {
    val newVertexPredicate:VertexPredicate[Component]=AnyMatchingComponentPredicate(vertexPredicate);
    val coarsedShortestPaths: Graph[DataMap, ED] =ShortestPathsAlgorithm.computeShortestPathsLengths(coarsedGraph,newVertexPredicate,treatAsUndirected)
    aproximatePaths(graph, coarsedGraph, coarsedShortestPaths,modifier,vertexPredicate,treatAsUndirected)
  }

  def aproximatePaths[ED: ClassTag, VD:ClassTag](graph: Graph[VD, ED], coarsedGraph: Graph[Component, ED], coarsedShortestPaths: Graph[DataMap, ED], modifier:PathModifier=defaultPathModifier, vertexPredicate: SimpleVertexPredicate= AllPathPredicate, treatAsUndirected:Boolean)(implicit num:Numeric[ED]) = {
    val modifiedPaths = coarsedShortestPaths.vertices.flatMap {
      case (vertexId: VertexId, paths: DataMap) => {
        paths.toIterable.map{
          case (to,len)=>(vertexId,(to.toLong,modifier(vertexId,to,len)))
        }
      }
    }.cache()
    val fromMapped=modifiedPaths.join(coarsedGraph.vertices).map{
      case (from,((to,len),componentFrom) )=>{
        (to,(componentFrom,len))
      }
    }.cache()

    val toJoined=fromMapped.join(coarsedGraph.vertices).cache()

    val toMapped=  toJoined.flatMap{
      case (to,((componentFrom,len),componentTo))=>{
        componentFrom.flatMap(
          (fromId)=>componentTo.map(
            (toId)=>(fromId,(toId,len))
          )
        )
      }
    }.cache()
    val interGroupPaths=toMapped.groupByKey()
    val outGraph =Graph(interGroupPaths, graph.edges,Iterable())
    val pathProcessors=new FastUtilWithDistance[VD,ED]();
    val pathCorrector= (correction:Double,excludeId:VertexId,data:Iterable[(VertexId,JDouble)])=>{
      val out =pathProcessors.getNewContainerForPaths()
      data.foreach{case (key,inValue)=>{
        if(key!=excludeId){
          out.put(key, inValue+correction)
        }
      }}
      out
    }
    val allVertexWithPaths: VertexRDD[DataMap] =outGraph.aggregateMessages[DataMap](
      (edgeCtx)=> {
        val mapToSrc= pathProcessors.mergePathContainers(pathCorrector(0,edgeCtx.srcId,edgeCtx.srcAttr),pathCorrector(1,edgeCtx.srcId,edgeCtx.dstAttr));
        if(vertexPredicate(edgeCtx.dstId)){
          mapToSrc.put(edgeCtx.dstId,1);
        }
        edgeCtx.sendToSrc(mapToSrc)
        if(treatAsUndirected){
          val mapToDst= pathProcessors.mergePathContainers(pathCorrector(0,edgeCtx.dstId,edgeCtx.dstAttr),pathCorrector(1,edgeCtx.dstId,edgeCtx.srcAttr));
          if(vertexPredicate(edgeCtx.srcId)){
            mapToDst.put(edgeCtx.srcId,1);
          }
          edgeCtx.sendToDst(mapToDst);
        }
      },
      pathProcessors.mergePathContainers
    )
    Graph(allVertexWithPaths,graph.edges,pathProcessors.getNewContainerForPaths())
  }

  def computeSingleShortestPathsLengths[VD:ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexId: VertexId, treatAsUndirected: Boolean = false, modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[DataMap,ED] = {
    computeShortestPathsLengthsUsing(graph,ByIdPredicate(vertexId),treatAsUndirected,modifier=defaultPathModifier)
  }

  def computeShortestPaths[VD:ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPredicate: SimpleVertexPredicate = AllPathPredicate, treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]) = {
    computeShortestPathsLengthsUsing(graph,vertexPredicate,treatAsUndirected,modifier=defaultPathModifier)
  }

  def computeShortestPathsLengthsIterativeUsing[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], coarsedGraph: Graph[Component, ED], bucketSizeProvider: BucketSizeProvider[Component,ED], treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[DataMap,ED] = {
    val coarsedShortestPaths: Graph[DataMap, ED] =ShortestPathsAlgorithm.computeShortestPathsLengthsIterative[Component,ED](coarsedGraph,bucketSizeProvider,treatAsUndirected)
    aproximatePaths(graph, coarsedGraph, coarsedShortestPaths,modifier,treatAsUndirected=treatAsUndirected)
  }

  def computeShortestPathsLengthsIterative[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], bucketSizeProvider: BucketSizeProvider[Component,ED], treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[DataMap,ED] = {
    val coarsedGraph=LPCoarsening.coarse(graph,treatAsUndirected)
    computeShortestPathsLengthsIterativeUsing(graph,coarsedGraph,bucketSizeProvider,treatAsUndirected)
  }

  def computeAPSPToDirectory[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], outDirectory: String, treatAsUndirected: Boolean, bucketSize:Long)(implicit num: Numeric[ED]): Unit = {
    val coarsedGraph=LPCoarsening.coarse(graph,treatAsUndirected)
    logger.info(s"Coarsed graph has size ${coarsedGraph.vertices.count()} in comparision to ${graph.vertices.count()}")
    val verticesGroups = graph.vertices.map(_._1).sortBy(k => k).collect().grouped(bucketSize.toInt).zipWithIndex.toList
    val numberOfIterations=verticesGroups.length;
    graph.cache()
    coarsedGraph.cache()
    (verticesGroups).foreach{
      case (group,iteration) => {
        logger.info(s"Approximated Shortest Paths iteration ${iteration+1} from  ${numberOfIterations}")
        val shortestPaths = ApproximatedShortestPathsAlgorithm.computeShortestPathsLengthsWithoutCoarsingUsing(graph,coarsedGraph, new ByIdsPredicate(group.toSet), treatAsUndirected)
        val joinedGraph = graph
          .outerJoinVertices(shortestPaths.vertices)((vId, data, newData) => (data, newData.getOrElse(new FastUtilWithDistance.DataMap)))
        joinedGraph.vertices.values.map {
          case (vertex, data: util.Map[JLong, JDouble]) => {
            val dataStr = data.entrySet()
              .map(e=>s"${e.getKey}:${e.getValue}").mkString(";")
            s"$vertex;$dataStr"
          }
        }.saveAsTextFile(s"${outDirectory}/from_${group.head}")
        shortestPaths.unpersist(blocking = false)
      }
    }

    graph.vertices.map(t => List(t._1, t._2).mkString(";")).saveAsTextFile(s"${outDirectory}/index")
  }

}

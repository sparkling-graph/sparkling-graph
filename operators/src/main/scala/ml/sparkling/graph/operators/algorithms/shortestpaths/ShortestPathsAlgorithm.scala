package ml.sparkling.graph.operators.algorithms.shortestpaths


import java.util

import ml.sparkling.graph.api.operators.IterativeComputation._
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes._
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.{FastUtilWithDistance, FastUtilWithPath}
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.{PathProcessor, SingleVertexProcessor}
import ml.sparkling.graph.operators.predicates.{AllPathPredicate, ByIdPredicate, ByIdsPredicate}
import org.apache.log4j.Logger
import org.apache.spark.graphx._

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

/**
 * Main object of shortest paths algorithm
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
case object ShortestPathsAlgorithm  {
  val logger=Logger.getLogger(ShortestPathsAlgorithm.getClass);
  /**
   * Path computing main method, should be used for further development and extension, object contains methods for main computations please use them instead of configuring this one
   * @param graph - graph for computation
   * @param pathProcessor - path processor that will handle path type dependent operations (processor for double, set etc.)
   * @param treatAsUndirected - treat graph as undirected (each path will be bidirectional)
   * @param num  - numeric to operate on  edge lengths
   * @tparam VD - vertex type
   * @tparam ED -  edge type
   * @tparam PT - path type
   * @return - Graph where each vertex contains all its shortest paths, type depends on path processor (double, list etc.)
   */
  def computeAllPathsUsing[VD, ED: ClassTag, PT: ClassTag](graph: Graph[VD, ED], vertexPredicate: VertexPredicate[VD], treatAsUndirected: Boolean, pathProcessor: PathProcessor[VD, ED, PT])(implicit num: Numeric[ED]) = {
    val initDistances = graph.aggregateMessages[PT](edgeContext => {
    if(vertexPredicate(edgeContext.dstId,edgeContext.dstAttr)){
        val edgeOut=pathProcessor.putNewPath(pathProcessor.getNewContainerForPaths(),edgeContext.dstId,edgeContext.attr)
        edgeContext.sendToSrc(edgeOut)
      }
      if(treatAsUndirected && vertexPredicate(edgeContext.srcId,edgeContext.srcAttr)){
        val edgeIn= pathProcessor.putNewPath(pathProcessor.getNewContainerForPaths(),edgeContext.srcId,edgeContext.attr)
        edgeContext.sendToDst(edgeIn)
      }
    },
      pathProcessor.mergePathContainers
    )
    val initMap: Graph[PT, ED] = graph.outerJoinVertices(initDistances)((vId, old, newValue) => newValue.getOrElse(pathProcessor.getNewContainerForPaths()))
    initMap.pregel[PT](pathProcessor.EMPTY_CONTAINER)(
      vprog = vertexProgram(pathProcessor),
      sendMsg = sendMessage(treatAsUndirected,pathProcessor),
      mergeMsg =  pathProcessor.mergePathContainers
    )
  }

  /**
   * Compute all pair shortest paths lengths (each vertex will contains map of shortest paths to other vertices)
   * @param graph
   * @param vertexPredicate - if true for vertexId, then distance to vertex is computed ,by default distances to all vertices are computed
   * @param treatAsUndirected - by default false, if true each edge is treated as bidirectional
   * @param num - numeric parameter for ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type (must be numeric)
   * @return graph where each vertex has map of its shortest paths lengths
   */
  def computeShortestPathsLengths[VD, ED: ClassTag](graph: Graph[VD, ED], vertexPredicate: VertexPredicate[VD] = AllPathPredicate, treatAsUndirected: Boolean = false)(implicit num: Numeric[ED]) = {
    computeAllPathsUsing(graph, vertexPredicate, treatAsUndirected, new FastUtilWithDistance[VD, ED]())
  }

  /**
   * Compute shortest paths lengths from all vertices to single one (each vertex will contain distance to given vertex)
   * @param graph
   * @param vertexId - vertex id to witch distances will be computed
   * @param treatAsUndirected - by default false, if true each edge is treated as bidirectional
   * @param num - numeric parameter for ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type (must be numeric)
   * @return graph where each vertex has distance to @vertexId
   */
  def computeSingleShortestPathsLengths[VD, ED: ClassTag](graph: Graph[VD, ED], vertexId: VertexId, treatAsUndirected: Boolean = false)(implicit num: Numeric[ED]) = {
    computeAllPathsUsing(graph, new ByIdPredicate(vertexId), treatAsUndirected, new SingleVertexProcessor[VD,ED](vertexId))
  }

  /**
   * Computes shoretest all pair shortest paths with paths (each vertex will has map of its paths to orher vertiecs,
   * map values are sets of paths (lists) where first element(0) is path length)
   * @param graph
   * @param vertexPredicate - if true for vertexId, then distance to vertex is computed ,by default distances to all vertices are computed
   * @param treatAsUndirected - by default false, if true each edge is treated as bidirectional
   * @param num - numeric parameter for ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type (must be numeric)
   * @return graph where each vertex has map of its shortest paths
   */

  def computeShortestPaths[VD, ED: ClassTag](graph: Graph[VD, ED], vertexPredicate: VertexPredicate[VD] = AllPathPredicate, treatAsUndirected: Boolean = false)(implicit num: Numeric[ED]) = {
    computeAllPathsUsing(graph, vertexPredicate, treatAsUndirected, new FastUtilWithPath[VD, ED]())
  }

  /**
   * Compute all pair shortest paths lengths (each vertex will contains map of shortest paths to other vertices) in multiple
   * super-steps of size provided by @bucketSizeProvider
   * @param graph
   * @param bucketSizeProvider - method that provides single super-step size
   * @param treatAsUndirected - by default false, if true each edge is treated as bidirectional
   * @param num - numeric parameter for ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type (must be numeric)
   * @return graph where each vertex has map of its shortest paths
   */
  def computeShortestPathsLengthsIterative[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], bucketSizeProvider: BucketSizeProvider[VD,ED], treatAsUndirected: Boolean = false,checkpointingFrequency:Int=100)(implicit num: Numeric[ED]) = {
    val bucketSize=bucketSizeProvider(graph)
    val vertexIds=graph.vertices.map{case (vId,data)=>vId}.collect()
    val outGraph:Graph[FastUtilWithDistance.DataMap ,ED] = graph.mapVertices((vId,data)=>new FastUtilWithDistance.DataMap)
    val vertices =vertexIds.grouped(bucketSize.toInt).toList
    val numberOfIterations=vertices.size
    val (out,_)=vertices.foldLeft((outGraph,1)){
      case ((acc,iteration),vertexIds)=>{
        logger.info(s"Shortest Paths iteration ${iteration} from  ${numberOfIterations}")
        val vertexPredicate=ByIdsPredicate(vertexIds.toSet)
        val computed=computeShortestPathsLengths(graph,vertexPredicate,treatAsUndirected)
        val outGraph=acc.outerJoinVertices(computed.vertices)((vId,outMap,computedMap)=>{
          computedMap.flatMap(m=>{outMap.putAll(m);Option(outMap)}).getOrElse(outMap)
        })
        if(iteration%checkpointingFrequency==0){
          logger.info(s"Chceckpointing graph")
          outGraph.checkpoint();
          outGraph.vertices.count();
          outGraph.edges.count();
        }
        (outGraph,iteration+1)
     }
    }
    out

  }

  private def sendMessage[VD, ED, PT](treatAsUndirected: Boolean, pathProcessor: PathProcessor[VD, ED, PT])(edge: EdgeTriplet[PT, ED])(implicit num: Numeric[ED]): Iterator[(VertexId, PT)] = {
    if (treatAsUndirected) {
      val extendedDst = pathProcessor.extendPaths(edge.srcId,edge.dstAttr, edge.dstId, edge.attr);
      val mergedSrc = pathProcessor.mergePathContainers(extendedDst, edge.srcAttr);
      val itSrc = if (!edge.srcAttr.equals(mergedSrc)) Iterator((edge.srcId, extendedDst)) else Iterator.empty
      val extendedSrc = pathProcessor.extendPaths(edge.dstId,edge.srcAttr, edge.srcId, edge.attr);
      val mergedDst = pathProcessor.mergePathContainers(extendedSrc, edge.dstAttr);
      val itDst = if (!edge.dstAttr.equals(mergedDst)) Iterator((edge.dstId, extendedSrc)) else Iterator.empty
      itSrc ++ itDst
    } else {
      val extendedDst = pathProcessor.extendPaths(edge.srcId,edge.dstAttr, edge.dstId, edge.attr);
      val merged = pathProcessor.mergePathContainers(extendedDst, edge.srcAttr);
      if (!edge.srcAttr.equals(merged)) Iterator((edge.srcId, extendedDst)) else Iterator.empty
    }
  }


  private def vertexProgram[VD, ED, PT](pathProcessor: PathProcessor[VD, ED, PT])(vId: VertexId, data: PT, message: PT)(implicit num: Numeric[ED]) = {
    pathProcessor.mergePathContainers(data, message)
  }

  def computeAPSPToDirectory[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], outDirectory: String, treatAsUndirected: Boolean, bucketSize:Long)(implicit num: Numeric[ED]): Unit = {
    val vertices= graph.vertices.map(_._1).sortBy(k => k).collect();
    val verticesGroups =vertices.grouped(bucketSize.toInt).zipWithIndex.toList
    val numberOfIterations=verticesGroups.length;
    graph.cache()
    (verticesGroups).foreach{
      case (group,iteration) => {
        logger.info(s"Shortest Paths iteration ${iteration+1} from  ${numberOfIterations}")
        val shortestPaths = ShortestPathsAlgorithm.computeShortestPathsLengths(graph, new ByIdsPredicate(group.toSet), treatAsUndirected)
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

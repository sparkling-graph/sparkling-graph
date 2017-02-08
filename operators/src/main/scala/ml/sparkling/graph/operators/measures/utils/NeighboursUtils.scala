package ml.sparkling.graph.operators.measures.utils

import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongOpenHashSet}
import ml.sparkling.graph.api.operators.IterativeComputation
import ml.sparkling.graph.api.operators.IterativeComputation.{SimpleVertexPredicate, VertexPredicate}
import ml.sparkling.graph.operators.predicates.AllPathPredicate
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Utitlity class that produces graphs where vertices are associated with maps of neighbours using it.unimi.dsi.fastutil as data store
 */
object NeighboursUtils {
  type NeighbourSet = LongOpenHashSet
  type NeighboursMap = Long2ObjectOpenHashMap[NeighbourSet]

  private def setWith(element: VertexId) = {
    val set = new NeighbourSet()
    set.add(element)
    set
  }

  private def mapWith(key: VertexId, value: NeighbourSet) = {
    val map = new NeighboursMap()
    map.put(key, value)
    map
  }

  /**
   *  Computes first level neighbours for each vertex
   * @param graph
   * @param treatAsUndirected
   * @param vertexPredicate
   * @tparam VD
   * @tparam ED
   * @return graph where vertex is associated with its first level neighbours
   */
  def getWithNeighbours[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                    treatAsUndirected: Boolean = false,
                                                    vertexPredicate: SimpleVertexPredicate = AllPathPredicate) = {

    val withNeighboursVertices = graph.mapVertices((vId,data)=>new NeighbourSet())
     .aggregateMessages[NeighbourSet](
        sendMsg=edgeContext=>{
          edgeContext.sendToSrc(if(vertexPredicate(edgeContext.dstId)) setWith(edgeContext.dstId) else new NeighbourSet(0))
          edgeContext.sendToDst(new NeighbourSet(0))
          if(treatAsUndirected){
            edgeContext.sendToDst(if (vertexPredicate(edgeContext.srcId)) setWith(edgeContext.srcId) else new NeighbourSet(0))
            edgeContext.sendToSrc(new NeighbourSet(0))
          }
        },
      mergeMsg = (s1,s2)=>{
      val out=s1.clone()
        out.addAll(s2)
        out
      }
      )
    graph
      .outerJoinVertices(withNeighboursVertices)((vId, oldValue, newValue) => newValue.getOrElse(new NeighbourSet(0)))
  }

  /**
   * Computes second level neighbours for each vertex
   * @param graph
   * @param treatAsUndirected
   * @param vertexPredicate
   * @tparam VD
   * @tparam ED
   * @return returns map of first level neighbours to their neighbours
   */
  def getWithSecondLevelNeighbours[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                               treatAsUndirected: Boolean = false,
                                                               vertexPredicate: SimpleVertexPredicate = AllPathPredicate) = {

    val withNeighboursVertices = getWithNeighbours(graph, treatAsUndirected, AllPathPredicate)
    val withSecondLevelNeighboursVertices = withNeighboursVertices
      .mapVertices((vId,neighbourSet)=>mapWith(vId,neighbourSet))
     .aggregateMessages[NeighboursMap](
        sendMsg=edgeContext=>{
          edgeContext.sendToSrc(if(vertexPredicate(edgeContext.dstId)) edgeContext.dstAttr else new NeighboursMap(0))
          edgeContext.sendToDst(new NeighboursMap(0))
          if(treatAsUndirected){
           edgeContext.sendToDst(if(vertexPredicate(edgeContext.srcId)) edgeContext.srcAttr else new NeighboursMap(0))
           edgeContext.sendToSrc(new NeighboursMap(0))
          }
        },
      mergeMsg = (s1,s2)=>{
      val out=s1.clone()
        out.putAll(s2)
        out
      }
      )
    graph
      .outerJoinVertices(withSecondLevelNeighboursVertices)((vId, oldValue, newValue) => newValue.getOrElse(new NeighboursMap(0)))
  }

}

package ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation

import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.{CoarseningAlgorithm, Component}
import org.apache.spark.graphx.{Edge, Graph, VertexId}

import scala.reflect.ClassTag

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 06.02.17.
  */
object LPCoarsening extends CoarseningAlgorithm{
  override def coarse[VD:ClassTag,ED:ClassTag](graph: Graph[VD, ED]): Graph[Component, ED] = {
    val uniqueEdges=graph.edges.map(edge=>Edge(Math.min(edge.srcId,edge.dstId),Math.max(edge.srcId,edge.dstId),edge.attr)).distinct();
    val filteredGraph=Graph(graph.vertices,uniqueEdges)
    val components=filteredGraph.mapVertices((vId,_)=>vId).pregel[List[VertexId]](List(),maxIterations = 2)(
      vprog = (vid,data,msg)=> {
        val commonList=(data::msg).groupBy(vId => vId).mapValues(l => l.length).toList.sortBy(t=>(t._2,t._1))
        commonList.lastOption.map {
          case (mostCommonId, _) => mostCommonId
        }.getOrElse(vid)
      },
      sendMsg = (e)=> Iterator((e.srcId,e.dstAttr::Nil),(e.dstId,e.srcAttr::Nil)),
      mergeMsg = (l1,l2)=> l1++l2

    );
    val newVertices=components.vertices.map{
      case (oldId,newId)=>(newId,oldId)
    }.groupByKey().map{
      case (newId,idsInComponent)=>(newId,idsInComponent.toList)
    }
    val newEdges=graph.edges.map(e=>(e.srcId,e))
      .join(components.vertices).map{
      case (oldSrc,(edge,newSrc))=>(edge.dstId,(edge,newSrc))
    }.join(components.vertices).map{
      case (oldDst,((edge,newSrc),newDst))=>Edge(newSrc,newDst,edge.attr)
    }.filter(e=>e.srcId!=e.dstId)

    Graph(newVertices,newEdges)
  }
}

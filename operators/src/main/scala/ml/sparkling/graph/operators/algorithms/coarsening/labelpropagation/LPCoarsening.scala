package ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation

import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.{CoarseningAlgorithm, Component}
import org.apache.spark.graphx.{Edge, Graph, VertexId}

import scala.reflect.ClassTag

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 06.02.17.
  */
object LPCoarsening extends CoarseningAlgorithm{
  override def coarse[VD:ClassTag,ED:ClassTag](graph: Graph[VD, ED],treatGraphAsUndirected:Boolean=false): Graph[Component, ED] = {
    val components=graph.mapVertices((vId,_)=>vId).pregel[List[VertexId]](List(),maxIterations = 2)(
      vprog = (vid,data,msg)=> {
        (data::msg).groupBy(vId => vId).mapValues(l => l.length).toList.sortBy(t=>(t._2,t._1)).lastOption.map {
          case (mostCommonId, _) => mostCommonId
        }.getOrElse(vid)
      },
      sendMsg = (e)=>
        if(treatGraphAsUndirected){
          Iterator((e.srcId,e.dstAttr::Nil),(e.dstId,e.srcAttr::Nil))
        }else{
          Iterator((e.dstId,e.srcAttr::Nil))
        },
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

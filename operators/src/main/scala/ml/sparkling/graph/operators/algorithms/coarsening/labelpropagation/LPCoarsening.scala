package ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation

import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.{CoarseningAlgorithm, Component}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 06.02.17.
  */

case class VertexWrapper(currentId:VertexId,changed:Boolean=false){
}
case object LPCoarsening extends CoarseningAlgorithm{
  override def coarse[VD:ClassTag,ED:ClassTag](graph: Graph[VD, ED],treatAsUndirected:Boolean=false): Graph[Component, ED] = {
    var iterationGraph: Graph[VertexWrapper, ED] =graph.mapVertices((vId, _)=>VertexWrapper(vId,true))
    var changed:Boolean=true;
    while(changed){
      val innerIterationGraph=iterationGraph.pregel[List[VertexId]](List.empty,maxIterations = 1)(
        vprog=(vId,data,msg)=>{
          if (msg.length==0) VertexWrapper(data.currentId,false) else
          (vId::msg).groupBy(id=>id).mapValues(_.length).toList.sortBy{
            case (id,count)=>(count,1.0/id)
          }.lastOption.map{
            case (id,count)=>VertexWrapper(id,data.currentId!=id)
          }.getOrElse(VertexWrapper(data.currentId,false))
        },
        sendMsg = (triplet)=>{
          val toDst= if(triplet.srcId!=triplet.srcAttr.currentId) Iterator.empty else Iterator((triplet.dstId,List(triplet.srcId)))
          val toSrc= if(triplet.dstId!=triplet.dstAttr.currentId) Iterator.empty else Iterator((triplet.srcId,List(triplet.dstId)))
          if(treatAsUndirected) toDst++toSrc else toDst
        },
        mergeMsg = (a,b)=>a++b
      ).cache()
     val innerIterationGraphSemi=innerIterationGraph.pregel[List[VertexId]](List.empty,maxIterations = 1)(
        vprog=(vId,data,msg)=>{
          if(msg.length==0) data else
          (msg).groupBy(id=>id).mapValues(_.length).get(data.currentId).map{
            case count=>VertexWrapper(data.currentId,data.changed)
          }.getOrElse(VertexWrapper(vId,vId!=data.currentId))
        },
        sendMsg = (triplet)=>{
          if(treatAsUndirected) Iterator((triplet.srcId,List(triplet.dstAttr.currentId)),(triplet.dstId,List(triplet.srcAttr.currentId))) else Iterator((triplet.dstId,List(triplet.srcAttr.currentId)))
        },
        mergeMsg = (a,b)=>a++b
      ).cache()
      changed=innerIterationGraphSemi.vertices.values.treeAggregate(false)((agg,data)=>agg||data.changed,_||_)
      iterationGraph=innerIterationGraphSemi;
    }
    val components: Graph[VertexId, ED] =iterationGraph.mapVertices{
      case (vertexId,VertexWrapper(newId,_))=>newId
    }
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

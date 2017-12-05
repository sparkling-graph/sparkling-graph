package ml.sparkling.graph.operators.algorithms.community.pscan

import org.apache.spark.graphx.{EdgeTriplet, Graph, Pregel, VertexId}

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 04.06.17.
  */
class PSCANConnectedComponents(minWeight:Double) extends Serializable{


  def run[VD,ED](graph:Graph[VertexId,Double], maxIterations:Int=Int.MaxValue):Graph[VertexId,Double]={
    val initialMessage = Long.MaxValue
    Pregel(graph, initialMessage,maxIterations = maxIterations)(
    vprog = (_, attr, msg) => math.min(attr, msg),
    sendMsg = sendMessage,
    mergeMsg = (a, b) => math.min(a, b))
  }

  def sendMessage(edge: EdgeTriplet[VertexId, Double]): Iterator[(VertexId, VertexId)] = {
    if(edge.attr > minWeight){
      if(edge.srcAttr<edge.dstAttr){
        Iterator((edge.dstId,edge.srcAttr))
      }else if(edge.dstAttr<edge.srcAttr){
        Iterator((edge.srcId,edge.dstAttr))
      }else{
        Iterator.empty
      }
    }else{
      Iterator.empty
    }
  }
}

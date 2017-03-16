package ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation

import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.{CoarseningAlgorithm, Component}
import org.apache.log4j.Logger
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 06.02.17.
  */

case class VertexWrapper(currentId:VertexId,beforeCurrentId:VertexId,beforeBeforeCurrent:VertexId,changed:Boolean=false){
}

case object ToNewId{
  def apply(id:VertexId,vertexWrapper: VertexWrapper)=vertexWrapper.currentId
}

case class ChangedStats(changed:Long,notChanged:Long){
  def + (other:ChangedStats):ChangedStats={
  ChangedStats(this.changed+other.changed,this.notChanged+other.notChanged)
  }
}

case object ChangedStatsAgregator{
  def add(b1:ChangedStats,b2:VertexWrapper)=if(b2.changed) ChangedStats(b1.changed+1,b1.notChanged) else ChangedStats(b1.changed,b1.notChanged+1)
  def add(b1:ChangedStats,b2:ChangedStats)=b1+b2
}
case object LPCoarsening extends CoarseningAlgorithm{
  val logger=Logger.getLogger(LPCoarsening.getClass())
  override def coarse[VD:ClassTag,ED:ClassTag](graph: Graph[VD, ED],treatAsUndirected:Boolean=false,checkpointingFrequency:Int=10): Graph[Component, ED] = {
    logger.info(s"Coarsing graph G using undirected parameter $treatAsUndirected")
    val filteredGraph=graph.filter(preprocess=(g:Graph[VD,ED])=>g,epred=(e:EdgeTriplet[VD, ED])=>e.srcId!=e.dstId).cache()
    var iterationGraph: Graph[VertexWrapper, ED] =filteredGraph.mapVertices((vId, _)=>VertexWrapper(vId,vId,vId,false))
    var changed:Boolean=true;
    var iteration=0
    while(changed){
      iteration+=1
      if(iteration % checkpointingFrequency==0){
        logger.info(s"Chceckpointing graph")
        iterationGraph.checkpoint()
        iterationGraph.vertices.foreachPartition((_)=>{})
        iterationGraph.edges.foreachPartition((_)=>{})
      }
      logger.debug(s"LPCoarsing loop $iteration")
      val innerIterationGraph=iterationGraph.pregel[List[VertexId]](List.empty,maxIterations = 1)(
        vprog=(vId,data,msg)=>{
          if (msg.length==0) VertexWrapper(data.currentId,data.beforeCurrentId,data.beforeBeforeCurrent,false) else
          (vId::msg).groupBy(id=>id).mapValues(_.length).toList.sortBy{
            case (id,count)=>(count,1.0/id)
          }.lastOption.map{
            case (id,count)=>VertexWrapper(id,data.currentId,data.beforeCurrentId,data.currentId!=id)
          }.getOrElse(VertexWrapper(data.currentId,data.beforeCurrentId,data.beforeBeforeCurrent,false))
        },
        sendMsg = (triplet)=>{
          val toDst= if(triplet.srcId!=triplet.srcAttr.currentId || triplet.dstAttr.currentId!=triplet.dstId) Iterator.empty else Iterator((triplet.dstId,List(triplet.srcId)))
          val toSrc= if(triplet.dstId!=triplet.dstAttr.currentId || triplet.srcAttr.currentId!=triplet.srcId) Iterator.empty else Iterator((triplet.srcId,List(triplet.dstId)))
          if(treatAsUndirected) toDst++toSrc else toDst
        },
        mergeMsg = (a,b)=>a++b
      ).cache()
     val innerIterationGraphSemi=innerIterationGraph.pregel[List[VertexId]](List.empty,maxIterations = 1)(
        vprog=(vId,data,msg)=>{
          if(msg.length==0) data else
          (msg).groupBy(id=>id).mapValues(_.length).get(data.currentId).map{
            case count=>data
          }.getOrElse({
            if(data.currentId==data.beforeBeforeCurrent){
              VertexWrapper(vId,data.currentId,data.beforeCurrentId,false)
            }  else
              VertexWrapper(vId,data.currentId,data.beforeCurrentId,vId!=data.currentId)
          })
        },
        sendMsg = (triplet)=>{
          if(treatAsUndirected) Iterator((triplet.srcId,List(triplet.dstAttr.currentId)),(triplet.dstId,List(triplet.srcAttr.currentId))) else Iterator((triplet.dstId,List(triplet.srcAttr.currentId)))
        },
        mergeMsg = (a,b)=>a++b
      ).cache()
      logger.debug(s"Looking if any vertice updated id during coarse")
      val changedStats=innerIterationGraphSemi.vertices.values.treeAggregate(ChangedStats(0,0))(ChangedStatsAgregator.add,ChangedStatsAgregator.add)
      logger.info(s"Vertices changed: ${changedStats.changed}, vertices not changed: ${changedStats.notChanged}")
      changed=changedStats.changed>0;
      iterationGraph=innerIterationGraphSemi.cache();
    }
    val components: Graph[VertexId, ED] =iterationGraph.mapVertices(ToNewId.apply)
    val newVertices=components.vertices.map{
      case (oldId,newId)=>(newId,oldId)
    }.groupByKey().map{
      case (newId,idsInComponent)=>(newId,idsInComponent.toList)
    }.cache()
    logger.info(s"Number of vertices after coarse: ${newVertices.count()}")
    val newEdges=graph.edges.map(e=>(e.srcId,e))
      .join(components.vertices).map{
      case (oldSrc,(edge,newSrc))=>(edge.dstId,(edge,newSrc))
    }.join(components.vertices).map{
      case (oldDst,((edge,newSrc),newDst))=>Edge(newSrc,newDst,edge.attr)
    }.filter(e=>e.srcId!=e.dstId).cache()
    Graph(newVertices,newEdges)
  }

}

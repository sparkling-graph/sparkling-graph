package ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation

import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.{CoarseningAlgorithm, Component, DefaultEdgeValueSelector, EdgeValueSelector}
import org.apache.log4j.Logger
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * Created by  Roman Bartusiak <riomus@gmail.com> on 06.02.17.
 */



case object SimpleLPCoarsening extends CoarseningAlgorithm{
  val logger=Logger.getLogger(SimpleLPCoarsening.getClass())
  override def coarse[VD:ClassTag,ED:ClassTag](graph: Graph[VD, ED],treatAsUndirected:Boolean=false,checkpointingFrequency:Int=50,edgeValueSelector:EdgeValueSelector=DefaultEdgeValueSelector): Graph[Component, ED] = {
    logger.info(s"Coarsing graph G using undirected parameter $treatAsUndirected")
    val components = graph.mapVertices((vId, _) => vId).pregel(Long.MaxValue)(
      vprog = (_: VertexId, data: VertexId, msgs: VertexId) => {
        if (msgs == Long.MaxValue) data else msgs
      },
      sendMsg = (triplet) => {
        val toDst = if(triplet.srcId<triplet.dstId && triplet.srcId == triplet.srcAttr && triplet.srcId < triplet.dstAttr ) (triplet.dstId, triplet.srcId) :: Nil else  Nil
        val toSrc = if(triplet.dstId<triplet.srcId  && triplet.dstId == triplet.dstAttr && triplet.dstId < triplet.srcAttr )  (triplet.srcId, triplet.dstId):: Nil else Nil
        val toDstReset = if (triplet.srcId != triplet.srcAttr && triplet.dstAttr == triplet.srcId ) (triplet.dstId, triplet.dstId):: Nil else Nil
        val toSrcReset = if (triplet.dstId != triplet.dstAttr && triplet.srcAttr == triplet.dstId ) (triplet.srcId, triplet.srcId):: Nil else Nil
        val result = if(treatAsUndirected) toDst ++ toDstReset ++ toSrc ++ toSrcReset else toDst ++ toDstReset
        result.iterator
      },
      mergeMsg = (id1, id2) => Math.min(id1, id2)
    )
    val newVertices=components.vertices.map{
      case (oldId,newId)=>(newId,oldId)
    }.groupByKey().map{
      case (newId,idsInComponent)=>(newId,idsInComponent.toList)
    }
    logger.info(s"Number of vertices after coarse: ${newVertices.count()}")
    val newEdges=graph.edges.map(e=>(e.srcId,e))
      .join(components.vertices, graph.vertices.partitions.length).map{
      case (_,(edge,newSrc))=>(edge.dstId,(edge,newSrc))
    }.join(components.vertices, graph.vertices.partitions.length).map{
      case (_,((edge,newSrc),newDst))=>Edge(newSrc,newDst,edge.attr)
    }.filter(e=>e.srcId!=e.dstId)
    Graph(newVertices,newEdges).groupEdges(edgeValueSelector.getValue)
  }

}

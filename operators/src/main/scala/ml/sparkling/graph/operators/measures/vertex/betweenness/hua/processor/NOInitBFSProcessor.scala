package ml.sparkling.graph.operators.measures.vertex.betweenness.hua.processor

import ml.sparkling.graph.operators.algorithms.bfs.processor.BFSProcessor
import ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.NOVertex
import ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.messages.{BFSConfirmMessage, BFSExpandMessage, NOMessage}
import org.apache.spark.graphx.{EdgeTriplet, VertexId}

/**
  * Created by mth on 5/7/17.
  */
class NOInitBFSProcessor[ED] extends BFSProcessor[NOVertex, ED, List[NOMessage[VertexId]]] {
  override def initialMessage: List[NOMessage[VertexId]] = List.empty

  override def mergeMessages(msg1: List[NOMessage[VertexId]], msg2: List[NOMessage[VertexId]]): List[NOMessage[VertexId]] = {
    val allMessages = msg1 ++ msg2
    val expandMessageList = allMessages.filter(_.isExpand)
    val expandMessage = expandMessageList.headOption
    val succMessages = allMessages.filter(_.isConfirm)

    expandMessage match {
      case Some(m) => succMessages :+ m
      case None => succMessages
    }
  }

  override def sendMessage(triplet: EdgeTriplet[NOVertex, ED]): Iterator[(VertexId, List[NOMessage[VertexId]])] = {

    def createExpandMsg(dstId: VertexId) = {
      val dstAttr = triplet.vertexAttr(dstId)
      val srcAttr = triplet.otherVertexAttr(dstId)
      if (dstAttr.pred.isEmpty && srcAttr.pred.nonEmpty) Iterator((dstId, List(BFSExpandMessage(triplet.otherVertexId(dstId))))) else Iterator.empty
    }

    def createConfirmMsg(dstId: VertexId) = {
      val dstAttr = triplet.vertexAttr(dstId)
      val srcAttr = triplet.otherVertexAttr(dstId)
      if (!dstAttr.isCompleted && srcAttr.pred.contains(dstId)) Iterator((dstId, List(BFSConfirmMessage(triplet.otherVertexId(dstId))))) else Iterator.empty
    }

    val confirmMsg = createConfirmMsg(triplet.srcId) ++ createConfirmMsg(triplet.dstId)
    val expandMsg = createExpandMsg(triplet.srcId) ++ createExpandMsg(triplet.dstId)
    confirmMsg ++ expandMsg
  }
}

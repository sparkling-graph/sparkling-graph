package ml.sparkling.graph.operators.algorithms.random.ctrw.processor

import ml.sparkling.graph.operators.algorithms.random.ctrw.factory.MessageFactory
import ml.sparkling.graph.operators.algorithms.random.ctrw.struct.{CTRWMessage, CTRWVertex}
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by mth on 4/13/17.
  */
class CTRWProcessor[VD, ED: ClassTag](graph: Graph[VD, ED], factory: MessageFactory) extends Serializable {

  lazy val initGraph = prepareRawGraph

  private def prepareRawGraph = {
    val nbsIds = graph.ops.collectNeighborIds(EdgeDirection.Either)
    graph.outerJoinVertices(nbsIds)((id, _, nbs) => CTRWVertex(id, nbs.getOrElse(Array.empty), Array.empty, initialized = false))
  }

  def createInitMessages(sampleSize: Int)(vertex: CTRWVertex) = {
    val msg = for (i <- 0 until sampleSize) yield factory.create(vertex)
    CTRWVertex(vertex.id, vertex.neighbours, msg.toArray)
  }

  def sendMessage(triplet: EdgeTriplet[CTRWVertex, ED]) = {

    def messagesTo(dest: VertexId) = {
      def messages = triplet.otherVertexAttr(dest).messages

      messages filter (_.nextVertex.contains(dest)) toList
    }

    Iterator((triplet.srcId, messagesTo(triplet.srcId))) ++ Iterator((triplet.dstId, messagesTo(triplet.dstId)))
  }

  def sendMessageCtx(round: Int)(edgeContext: EdgeContext[CTRWVertex, _, List[CTRWMessage]]) = {
    val triplet = edgeContext.toEdgeTriplet

    def messagesTo(dest: VertexId) = {
      def messages = triplet.otherVertexAttr(dest).messages

      messages filter (_.nextVertex.contains(dest)) toList
    }

    def send(msg: List[CTRWMessage], f: (List[CTRWMessage]) => Unit) =
      if (msg.nonEmpty) f(msg)

    send(messagesTo(edgeContext.srcId), edgeContext.sendToSrc)
    send(messagesTo(edgeContext.dstId), edgeContext.sendToDst)
  }

  def mergeMessages(msg1: List[CTRWMessage], msg2: List[CTRWMessage]) = msg1 ++ msg2

  def applyMessages(round: Int)(vertexId: VertexId, data: CTRWVertex, messagesOps: Option[List[CTRWMessage]]) = {
    val newMessages = messagesOps match {
      case Some(messages) => messages map (factory.correct(data, _))
      case None => List.empty
    }
    val keptMessages = data.messages filter (_.nextVertex.isEmpty)
    CTRWVertex(vertexId, data.neighbours, newMessages.toArray ++ keptMessages)
  }
}

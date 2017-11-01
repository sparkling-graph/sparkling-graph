package ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.processor

import ml.sparkling.graph.operators.algorithms.bfs.processor.BFSProcessor
import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct.EdmondsVertex
import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct.messages.EdmondsMessage
import org.apache.spark.graphx.{EdgeTriplet, VertexId}

/**
  * Created by mth on 3/15/17.
  */
class EdmondsBCProcessor[ED] extends BFSProcessor[EdmondsVertex, ED, EdmondsMessage] {

  override def initialMessage: EdmondsMessage = EdmondsMessage.empty

  override def mergeMessages(msg1: EdmondsMessage, msg2: EdmondsMessage): EdmondsMessage =
    msg1.merge(msg2)

  override def sendMessage(triplet: EdgeTriplet[EdmondsVertex, ED]): Iterator[(VertexId, EdmondsMessage)] = {

    def msgIterator(currentVertexId: VertexId) = {
      val othAttr = triplet.otherVertexAttr(currentVertexId)
      val thisAttr = triplet.vertexAttr(currentVertexId)
      if (othAttr.explored) Iterator.empty else Iterator((triplet.otherVertexId(currentVertexId), EdmondsMessage(List(currentVertexId), thisAttr.sigma, thisAttr.depth + 1)))
    }

    def hasParent(source: VertexId) = triplet.vertexAttr(source).explored

    val srcMsg = if (hasParent(triplet.srcId)) msgIterator(triplet.srcId) else Iterator.empty
    val dstMsg = if (hasParent(triplet.dstId)) msgIterator(triplet.dstId) else Iterator.empty
    srcMsg ++ dstMsg
  }
}

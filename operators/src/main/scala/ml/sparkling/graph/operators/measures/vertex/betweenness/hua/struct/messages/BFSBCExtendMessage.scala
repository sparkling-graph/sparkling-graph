package ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.messages

import ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.NOBFSVertex
import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 5/9/17.
  */
class BFSBCExtendMessage(val source: VertexId, val distance: Double, val sigma: Int) extends NOMessage[VertexId] {
  override def content: VertexId = source

  override val isExpand: Boolean = true
}

object BFSBCExtendMessage extends Serializable {
  def apply(source: VertexId, distance: Double, sigma: Int): BFSBCExtendMessage =
    new BFSBCExtendMessage(source, distance, sigma)

  def create(source: VertexId, vertex: NOBFSVertex): BFSBCExtendMessage =
    new BFSBCExtendMessage(source, vertex.distance + 1, vertex.sigma)
}

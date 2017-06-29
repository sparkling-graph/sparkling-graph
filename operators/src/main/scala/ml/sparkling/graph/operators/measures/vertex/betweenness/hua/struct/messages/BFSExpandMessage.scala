package ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.messages

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 5/6/17.
  */
class BFSExpandMessage(val parent: VertexId) extends NOMessage[VertexId] {
  override val source: VertexId = parent

  override val content: VertexId = parent

  override def isExpand = true
}

object BFSExpandMessage extends Serializable {
  def apply(parent: VertexId): BFSExpandMessage = new BFSExpandMessage(parent)

  def unapply(arg: BFSExpandMessage): Option[VertexId] = Some(arg.parent)
}
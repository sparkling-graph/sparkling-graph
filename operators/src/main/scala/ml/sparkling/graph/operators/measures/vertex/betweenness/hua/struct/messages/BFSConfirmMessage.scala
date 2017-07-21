package ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.messages

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 5/6/17.
  */
class BFSConfirmMessage(val child: VertexId) extends NOMessage[VertexId] {
  override val source: VertexId = child

  override val content: VertexId = child

  override def isConfirm = true
}

object BFSConfirmMessage extends Serializable {
  def apply(child: VertexId): BFSConfirmMessage = new BFSConfirmMessage(child)

  def unapply(arg: BFSConfirmMessage): Option[VertexId] = Some(arg.child)
}

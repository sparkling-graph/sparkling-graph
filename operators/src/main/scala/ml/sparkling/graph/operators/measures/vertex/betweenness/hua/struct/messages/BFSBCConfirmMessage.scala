package ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.messages

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 6/11/17.
  */
class BFSBCConfirmMessage(val source: VertexId) extends NOMessage[VertexId]{

  override def content: VertexId = source

  override def isConfirm: Boolean = true
}

object BFSBCConfirmMessage extends Serializable {
  def apply(source: VertexId): BFSBCConfirmMessage = new BFSBCConfirmMessage(source)
}
package ml.sparkling.graph.operators.algorithms.random.ctrw.struct

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 4/13/17.
  */
class CTRWVertex(val id: VertexId, val neighbours: Array[VertexId], val messages: Array[CTRWMessage], val initialized: Boolean) extends Serializable {
  val degree = neighbours.length
}

object CTRWVertex extends Serializable {
  def apply(id: VertexId, neighbours: Array[VertexId], messages: Array[CTRWMessage] = Array.empty, initialized: Boolean = true): CTRWVertex =
    new CTRWVertex(id, neighbours, messages, initialized)

  def unapply(arg: CTRWVertex): Option[(VertexId, Array[VertexId], Array[CTRWMessage], Boolean)] = Some((arg.id, arg.neighbours, arg.messages, arg.initialized))
}

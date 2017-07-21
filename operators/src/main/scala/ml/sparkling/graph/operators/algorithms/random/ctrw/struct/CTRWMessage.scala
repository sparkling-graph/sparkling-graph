package ml.sparkling.graph.operators.algorithms.random.ctrw.struct

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 4/13/17.
  */
class CTRWMessage(val src: VertexId, val temp: Double, val nextVertex: Option[VertexId] = None) extends Serializable

object CTRWMessage extends Serializable {
  def apply(src: VertexId, temp: Double, nextVertex: Option[VertexId] = None): CTRWMessage =
    new CTRWMessage(src, temp, nextVertex)

  def unapply(arg: CTRWMessage): Option[(VertexId, Double, Option[VertexId])] =
    Some((arg.src, arg.temp, arg.nextVertex))
}

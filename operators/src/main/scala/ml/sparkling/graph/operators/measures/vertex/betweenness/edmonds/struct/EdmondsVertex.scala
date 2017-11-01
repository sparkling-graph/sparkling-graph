package ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct

import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct.messages.EdmondsMessage
import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 3/13/17.
  */
class EdmondsVertex(val preds: List[VertexId], val sigma: Int, val depth: Int, val delta: Double, val bc:Double) extends Serializable {
  val explored = preds.nonEmpty

  def applyMessage(msg: EdmondsMessage): EdmondsVertex = EdmondsVertex(msg.preds, msg.sigma, msg.depth)

  override def toString = s"EdmondsVertex($sigma, $depth, $delta, $bc)"
}

object EdmondsVertex {
  def apply(preds: List[VertexId] = List.empty, sigma: Int = -1, depth: Int = -1, delta: Double = 0.0, bc:Double = 0.0): EdmondsVertex =
    new EdmondsVertex(preds, sigma, depth, delta, bc)

  def unapply(arg: EdmondsVertex): Option[(List[VertexId], Int, Int, Double, Double)] = Some((arg.preds, arg.sigma, arg.depth, arg.delta, arg.bc))
}
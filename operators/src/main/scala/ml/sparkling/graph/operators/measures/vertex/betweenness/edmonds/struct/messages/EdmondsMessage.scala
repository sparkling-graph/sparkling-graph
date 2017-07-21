package ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct.messages

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 7/9/17.
  */
class EdmondsMessage(val preds: List[VertexId], val sigma: Int, val depth: Int) extends Serializable {
  def merge(other: EdmondsMessage): EdmondsMessage = {
    require(depth == other.depth)
    EdmondsMessage(preds ++ other.preds, sigma + other.sigma, depth)
  }
}

object EdmondsMessage extends Serializable {
  def apply(preds: List[VertexId], sigma: Int, depth: Int): EdmondsMessage =
    new EdmondsMessage(preds, sigma, depth)

  def empty = apply(List.empty, -1, -1)
}

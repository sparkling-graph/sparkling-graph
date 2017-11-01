package ml.sparkling.graph.operators.algorithms.bfs.processor

import org.apache.spark.graphx.{EdgeTriplet, VertexId}

/**
  * Created by mth on 6/26/17.
  */
trait BFSProcessor[VD, ED, MD] extends Serializable {
  def initialMessage: MD
  def sendMessage(triplet: EdgeTriplet[VD, ED]): Iterator[(VertexId, MD)]
  def mergeMessages(msg1: MD, msg2: MD): MD
}

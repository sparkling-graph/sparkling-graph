package ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.messages

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 5/6/17.
  */
trait NOMessage[MVT] extends Serializable {
  def source: VertexId
  def content: MVT

  def isExpand = false
  def isConfirm = false
  def isDFSPointer = false
  def isAggregation = false
}

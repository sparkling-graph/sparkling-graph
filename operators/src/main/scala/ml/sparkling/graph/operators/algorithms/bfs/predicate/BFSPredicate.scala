package ml.sparkling.graph.operators.algorithms.bfs.predicate

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 6/26/17.
  */
trait BFSPredicate[VD, MD] extends Serializable {
  def getInitialData(vertexId: VertexId, attr: VD): (VertexId) => VD
  def applyMessages(vertexId: VertexId, date: VD, message: MD): VD
}

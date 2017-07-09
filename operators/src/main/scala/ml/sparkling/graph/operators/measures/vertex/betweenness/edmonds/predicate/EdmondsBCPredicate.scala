package ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.predicate

import ml.sparkling.graph.operators.algorithms.bfs.predicate.BFSPredicate
import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct.EdmondsVertex
import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct.messages.EdmondsMessage
import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 3/15/17.
  */
class EdmondsBCPredicate extends BFSPredicate[EdmondsVertex, EdmondsMessage] {

  override def getInitialData(vertexId: VertexId, attr: EdmondsVertex): (VertexId) => EdmondsVertex =
    (vId) => if (vId == vertexId) EdmondsVertex(List(vId), 1, 0) else EdmondsVertex()

  override def applyMessages(vertexId: VertexId, date: EdmondsVertex, message: EdmondsMessage): EdmondsVertex =
    if (date.explored) date else date.applyMessage(message)

}

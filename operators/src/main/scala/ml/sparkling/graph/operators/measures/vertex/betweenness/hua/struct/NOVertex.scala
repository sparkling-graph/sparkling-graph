package ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct

import ml.sparkling.graph.operators.measures.vertex.betweenness.hua.struct.messages.DFSPointer
import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 5/6/17.
  */
class NOVertex(val vertexId: VertexId,
               val bfsMap: Map[VertexId, NOBFSVertex],
               val pred: Option[VertexId],
               val succ: Option[Array[VertexId]],
               val dfsPointer: Option[DFSPointer],
               val bc: Double) extends Serializable {
  def setParent(idParent: VertexId) = NOVertex(vertexId, bfsMap, Some(idParent), succ, dfsPointer, bc)

  def setPredecessorAndSuccessors(newPred: Option[VertexId], newSucc: Option[Array[VertexId]]) =
    NOVertex(vertexId, bfsMap, newPred, newSucc, dfsPointer, bc)

  val isCompleted = pred.nonEmpty && succ.nonEmpty

  val leaf = succ.isEmpty

  lazy val bfsRoot = bfsMap.contains(vertexId)

  lazy val lowestSucc = succ.getOrElse(Array.empty).sorted.headOption

  lazy val eccentricity = if (bfsMap.isEmpty) 0 else bfsMap.map({ case (id, v) => v.distance}).max

  def withDfsPointer(pointer: Option[DFSPointer]) =
    NOVertex(vertexId, bfsMap, pred, succ, pointer, bc)

  def update(bfsMap: Map[VertexId, NOBFSVertex] = bfsMap, succ: Option[Array[VertexId]] = succ, dfsPointer: Option[DFSPointer] = dfsPointer, bcInc: Double = 0) =
    NOVertex(vertexId, bfsMap, pred, succ, dfsPointer, bc + bcInc)
}

object NOVertex extends Serializable {
  def apply(vertexId: VertexId,
            bfsMap: Map[VertexId, NOBFSVertex] = Map.empty,
            pred: Option[VertexId] = None,
            succ: Option[Array[VertexId]] = None,
            dfsPointer: Option[DFSPointer] = None,
            bc: Double = .0): NOVertex = new NOVertex(vertexId, bfsMap, pred, succ, dfsPointer, bc)
}

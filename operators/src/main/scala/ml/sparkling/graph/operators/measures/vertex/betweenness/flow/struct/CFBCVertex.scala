package ml.sparkling.graph.operators.measures.vertex.betweenness.flow.struct

import org.apache.spark.graphx.VertexId

/**
  * Created by mth on 4/23/17.
  */
class CFBCVertex(
                  val id: VertexId,
                  val degree: Int,
                  val bc: Double,
                  val sampleVertices: Array[VertexId],
                  val flows: (Array[CFBCFlow], Iterable[CFBCNeighbourFlow]),
                  val processedFlows: Int) extends Serializable {
  lazy val relatedFlows = vertexFlows.filter(f => f.dst == id || f.src == id)
  lazy val availableSamples = sampleVertices

  lazy val vertexPhi = vertexFlows.count(_.src == id)

  lazy val flowsMap = vertexFlows.map(f => ((f.src, f.dst), f)).toMap

  val (vertexFlows, neighboursFlows) = flows

  def isFinalized(k: Int) = sampleVertices.isEmpty || processedFlows >= k

  def getFlow(key: (VertexId, VertexId)) = flowsMap.getOrElse(key, CFBCFlow.empty(key))

  def updateBC(currentFlowing: Double) = {
    val newBC = (processedFlows * bc + currentFlowing) / (processedFlows + 1)
    new CFBCVertex(id, degree, newBC, sampleVertices, flows, processedFlows + 1)
  }

  def updateBC(currentFlowing: Seq[Double]) = {
    val newBC = if (currentFlowing.isEmpty) bc else (processedFlows * bc + currentFlowing.sum) / (processedFlows + currentFlowing.length)
    new CFBCVertex(id, degree, newBC, sampleVertices, flows, processedFlows + currentFlowing.length)
  }

  def addNewFlow(flow: CFBCFlow) =
    new CFBCVertex(id, degree, bc, sampleVertices.filterNot(_ == flow.dst), (vertexFlows :+ flow, neighboursFlows), processedFlows)

  def updateFlows(fls: Array[CFBCFlow]) =
    new CFBCVertex(id, degree, bc, sampleVertices, (fls, neighboursFlows), processedFlows)

  def removeFlows(toRemove: Seq[CFBCFlow]) = {
    val newFlows = vertexFlows.diff(toRemove).map(_.countdownVitality)
    new CFBCVertex(id, degree, bc, sampleVertices, (newFlows, neighboursFlows), processedFlows)
  }

  def applyNeighbourFlows(nbhFlows: Iterable[CFBCNeighbourFlow]) =
    new CFBCVertex(id, degree, bc, sampleVertices, (vertexFlows, nbhFlows), processedFlows)
}

object CFBCVertex extends Serializable {
  def apply(id: VertexId,
            degree: Int,
            bc: Double = 0.0,
            sampleVertices: Array[VertexId] = Array.empty,
            flows: (Array[CFBCFlow], Iterable[CFBCNeighbourFlow]) = (Array.empty, Iterable.empty)
           ): CFBCVertex = new CFBCVertex(id, degree, bc, sampleVertices, flows, 0)
}

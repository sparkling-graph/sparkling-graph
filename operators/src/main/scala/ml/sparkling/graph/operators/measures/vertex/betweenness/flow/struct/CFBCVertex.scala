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
  lazy val relatedFlows = flows._1.filter(f => f.dst == id || f.src == id)
  lazy val availableSamples = sampleVertices/*.diff(flows._1.filter(_.src == id).map(_.dst) :+ id)*/

  lazy val vertexPhi = flows._1.count(_.src == id)

  lazy val flowsMap = flows._1.map(f => ((f.src, f.dst), f)).toMap

  val vertexFlows = flows._1
  val neighboursFlows = flows._2

  def isFinalized(k: Int) = sampleVertices.isEmpty || processedFlows >= k

  def getFlow(key: (VertexId, VertexId)) = flowsMap.getOrElse(key, CFBCFlow.empty(key._1, key._2))

  def updateBC(currentFlowing: Double) = {
    val newBC = (processedFlows * bc + currentFlowing) / (processedFlows + 1)
    new CFBCVertex(id, degree, newBC, sampleVertices, flows, processedFlows + 1)
  }

  def updateBC(currentFlowing: Seq[Double]) = {
    val newBC = if (currentFlowing.isEmpty) bc else (processedFlows * bc + currentFlowing.sum) / (processedFlows + currentFlowing.length)
    new CFBCVertex(id, degree, newBC, sampleVertices, flows, processedFlows + currentFlowing.length)
  }

  def addNewFlow(flow: CFBCFlow) =
    new CFBCVertex(id, degree, bc, sampleVertices.filterNot(_ == flow.dst), (flows._1 :+ flow, flows._2), processedFlows)

  def updateFlows(fls: Array[CFBCFlow]) =
    new CFBCVertex(id, degree, bc, sampleVertices, (fls, flows._2), processedFlows)

  def removeFlows(toRemove: Seq[CFBCFlow]) = {
    val newFlows = flows._1.diff(toRemove).map(_.countdownVitality)
    new CFBCVertex(id, degree, bc, sampleVertices, (newFlows, flows._2), processedFlows)
  }

  def applyNeighbourFlows(nbhFlows: Iterable[CFBCNeighbourFlow]) =
    new CFBCVertex(id, degree, bc, sampleVertices, (flows._1, nbhFlows), processedFlows)
}

object CFBCVertex extends Serializable {
  def apply(id: VertexId,
            degree: Int,
            bc: Double = 0.0,
            sampleVertices: Array[VertexId] = Array.empty,
            flows: (Array[CFBCFlow], Iterable[CFBCNeighbourFlow]) = (Array.empty, Iterable.empty)
           ): CFBCVertex = new CFBCVertex(id, degree, bc, sampleVertices, flows, 0)
}

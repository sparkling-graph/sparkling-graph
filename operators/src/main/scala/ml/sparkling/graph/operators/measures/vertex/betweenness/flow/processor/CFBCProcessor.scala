package ml.sparkling.graph.operators.measures.vertex.betweenness.flow.processor

import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.generator.FlowGenerator
import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.struct.{CFBCFlow, CFBCNeighbourFlow, CFBCVertex}
import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId, VertexRDD}

import scala.reflect.ClassTag

/**
  * Created by mth on 4/23/17.
  */
class CFBCProcessor[VD, ED: ClassTag](graph: Graph[VD, ED], flowGenerator: FlowGenerator[CFBCVertex, Option[CFBCFlow]]) extends Serializable {

  lazy val initGraph = prepareRawGraph

  lazy val numOfVertices = graph.ops.numVertices

  private def prepareRawGraph = {
    val degrees = graph.ops.degrees
    graph.outerJoinVertices(degrees)((id, _, deg) => CFBCVertex(id, deg.getOrElse(0)))
  }

  def createFlow(graph: Graph[CFBCVertex, ED]) = {
    graph.mapVertices((id, data) =>
      flowGenerator.createFlow(data) match {
        case Some(flow) => data.addNewFlow(flow)
        case None => data
      })
  }

  def joinReceivedFlows(vertexId: VertexId, vertex: CFBCVertex, msg: Array[CFBCFlow]) =
    vertex.applyNeighbourFlows(msg.groupBy(_.key).map(it => if (it._2.nonEmpty) CFBCNeighbourFlow(it._2, vertex) else CFBCNeighbourFlow(it._1._1, it._1._2)))


  def applyFlows(epsilon: Double)(id: VertexId, data: CFBCVertex) = {

    def updateFlow(contextFlow: CFBCFlow, nbhFlow: CFBCNeighbourFlow) = {
      val newPotential = (nbhFlow.sumOfPotential + contextFlow.supplyValue(id)) / data.degree
      val potentialDiff = Math.abs(contextFlow.potential - newPotential)
      val completed = contextFlow.completed || potentialDiff < epsilon
      CFBCFlow(contextFlow.src, contextFlow.dst, newPotential, completed, contextFlow.aliveThrough)
    }

    val newFlows = for (nb <- data.neighboursFlows) yield {
      val flowOpt = data.flowsMap.get(nb.key)
      flowOpt match {
        case Some(flow) => Some(updateFlow(flow, nb))
        case None if !nb.anyCompleted => Some(updateFlow(CFBCFlow.empty(nb.key._1, nb.key._2), nb))
        case _ => None
      }
    }

    val k2 = newFlows.filter(_.nonEmpty).map(f => (f.get.key, f.get)).toMap

    data.updateFlows((data.flowsMap ++ k2).values.toArray)
  }

  def computeBetweenness(vertexId: VertexId, vertex: CFBCVertex) = {
    val applicableFlows = vertex.neighboursFlows.filter(nf => nf.src != vertexId && nf.dst != vertexId)
    val completedReceivedFlows = applicableFlows.filter(_.allCompleted)
    val completedFlows = completedReceivedFlows.filter(nf => vertex.getFlow(nf.key).completed)

    val currentFlows = completedFlows.map(_.sumOfDifferences / 2)

    vertex.updateBC(currentFlows.toSeq)
  }

  def removeCompletedFlows(vertexId: VertexId, vertex: CFBCVertex) = {
    val completedReceivedFlows = vertex.neighboursFlows.map(nf => (nf.key, nf.allCompleted)).toMap
    val completedFlows = vertex.vertexFlows.filter(f => f.removable && completedReceivedFlows.getOrElse(f.key, true))

    vertex.removeFlows(completedFlows)
  }

  def extractFlowMessages(graph: Graph[CFBCVertex, ED]) =
    graph.aggregateMessages[Array[CFBCFlow]](ctx => {

      def send(triplet: EdgeTriplet[CFBCVertex, ED])(dst: VertexId, sendF: (Array[CFBCFlow]) => Unit): Unit = {
        val srcFlows = triplet.otherVertexAttr(dst).vertexFlows
        val dstFlowsKeys = triplet.vertexAttr(dst).vertexFlows.map(_.key).toSet
        val activeFlows = srcFlows.filterNot(_.completed)
        val completedFlows = srcFlows.filter(f => f.completed && dstFlowsKeys.contains(f.key))
        sendF(activeFlows ++ completedFlows)
      }

      val sendDataTo = send(ctx.toEdgeTriplet) _
      sendDataTo(ctx.srcId, ctx.sendToSrc)
      sendDataTo(ctx.dstId, ctx.sendToDst)
    }, _ ++ _)

  def preMessageExtraction(eps: Double)(graph: Graph[CFBCVertex, ED], msg: VertexRDD[Array[CFBCFlow]]) =
    graph.outerJoinVertices(msg)((vertexId, vertex, vertexMsg) => {
      val newVert = joinReceivedFlows(vertexId, vertex, vertexMsg.getOrElse(Array.empty))
      applyFlows(eps)(vertexId, newVert)
    })

  def postMessageExtraction(graph: Graph[CFBCVertex, ED]) =
    graph.mapVertices((id, vertex) => {
      val vertWithBC = computeBetweenness(id, vertex)
      removeCompletedFlows(id, vertWithBC)
    })

}

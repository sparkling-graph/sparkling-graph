package ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds

import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct.EdmondsVertex
import org.apache.spark.graphx.{VertexRDD, _}

/**
  * Created by mth on 4/12/17.
  */
class EdmondsBCAggregator[ED] extends Serializable {

  def aggregate(graph: Graph[EdmondsVertex, ED], source: VertexId) = {
//    val startTime = System.nanoTime()

    val maxDepth = graph.vertices.aggregate(0)((depth, v) => Math.max(v._2.depth, depth), Math.max)

    var g = graph
    var oldGraph: Graph[EdmondsVertex, ED] = null

    var messages = aggregateMessages(g, maxDepth).cache
    messages.count

    for (i <- 1 until maxDepth reverse) {
      oldGraph = g

      g = applyMessages(oldGraph, messages).cache
      val oldMessages = messages
      messages = aggregateMessages(g, i).cache
      messages.count

      oldMessages.unpersist(false)
      oldGraph.unpersistVertices(false)
      oldGraph.edges.unpersist(false)
    }

    messages.unpersist(false)

//    println("Time of execution updateCentrality:" + ((finishTime - startTime) / 1000000) + " ms")
    //    val finishTime = System.nanoTime()
    g
  }

  private def aggregateMessages(graph: Graph[EdmondsVertex, ED], depth: Int) = graph.aggregateMessages[Double](
    edgeContext => {
      val sender = createAndSendMessage(edgeContext.toEdgeTriplet, depth) _
      sender(edgeContext.srcId, edgeContext.sendToDst)
      sender(edgeContext.dstId, edgeContext.sendToSrc)
    }, _ + _
  )

  private def createAndSendMessage(triplet: EdgeTriplet[EdmondsVertex, ED], depth: Int)(source: VertexId, f: (Double) => Unit) = {
    val attr = triplet.vertexAttr(source)
    if (attr.depth == depth) sendMessage(produceMessage(triplet)(source), f)
  }

  private def produceMessage(triplet: EdgeTriplet[EdmondsVertex, ED])(source: VertexId) = {
    val attr = triplet.vertexAttr(source)
    val otherAttr = triplet.otherVertexAttr(source)
    val delta = (otherAttr.sigma.toDouble / attr.sigma.toDouble) * (1.0 + attr.delta)
    if (attr.preds.contains(triplet.otherVertexId(source))) Some(delta) else None
  }

  private def sendMessage(message: Option[Double], f: (Double) => Unit) = message.foreach(f)

  private def applyMessages(graph: Graph[EdmondsVertex, ED], messages: VertexRDD[Double]) =
    graph.ops.joinVertices(messages)((vertexId, attr, delta) => {
      EdmondsVertex(attr.preds, attr.sigma, attr.depth, delta, delta)
    })
}

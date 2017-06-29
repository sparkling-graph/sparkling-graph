package ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds

import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct.EdmondsVertex
import ml.sparkling.graph.operators.utils.BetweennessUtils
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by mth on 3/12/17.
  */

class EdmondsBC[VD, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  lazy val simpleGraph = prepareRawGraph

  val bcAggregator = new EdmondsBCAggregator[ED]

  private def prepareRawGraph = {
    graph.mapVertices((vId, attr) => EdmondsVertex())
  }

  def computeBC = {

    val verticesIds = simpleGraph.vertices.map({ case (vertexId, _) => vertexId }).cache
    val verticesIterator = verticesIds.toLocalIterator
    var betweennessVector: VertexRDD[Double] = simpleGraph.vertices.mapValues(_ => .0).cache()
    var i = 0

    while (verticesIterator.hasNext) {
      val processedVertex = verticesIterator.next

      val bfsSP = bfs(simpleGraph, processedVertex)
      val computedGraph = bcAggregator.aggregate(bfsSP, processedVertex)

      val partialBetweennessVector = computedGraph.vertices.mapValues(_.bc)

      val previousBetweennessVector = betweennessVector
      betweennessVector = updateBC(betweennessVector, partialBetweennessVector)

      betweennessVector.checkpoint()
      betweennessVector.count
      previousBetweennessVector.unpersist(false)

      bfsSP.unpersist(false)
      computedGraph.unpersistVertices(false)
      computedGraph.edges.unpersist(false)

      i = i + 1
    }

    verticesIds.unpersist(false)
    finalize(betweennessVector)
  }

  private def bfs(graph: Graph[EdmondsVertex, ED], startVertex: VertexId) = {

    def applyMessages(vertexId: VertexId, data: EdmondsVertex, message: (List[VertexId], Int, Int)): EdmondsVertex =
      if (data.explored) data else EdmondsVertex(message._1, message._2, message._3)

    def sendMessage(triplet: EdgeTriplet[EdmondsVertex, ED]): Iterator[(VertexId, (List[VertexId], Int, Int))] = {

      def msgIterator(currentVertexId: VertexId) = {
        val othAttr = triplet.otherVertexAttr(currentVertexId)
        val thisAttr = triplet.vertexAttr(currentVertexId)
        if (othAttr.explored) Iterator.empty else Iterator((triplet.otherVertexId(currentVertexId), (List(currentVertexId), thisAttr.sigma, thisAttr.depth + 1)))
      }

      def hasParent(source: VertexId) = triplet.vertexAttr(source).explored

      val srcMsg = if (hasParent(triplet.srcId)) msgIterator(triplet.srcId) else Iterator.empty
      val dstMsg = if (hasParent(triplet.dstId)) msgIterator(triplet.dstId) else Iterator.empty
      srcMsg ++ dstMsg
    }

    def mergeMessages(msg1: (List[VertexId], Int, Int), msg2: (List[VertexId], Int, Int)): (List[VertexId], Int, Int) = {
      require(msg1._3 == msg2._3)
      (msg1._1 ++ msg2._1, msg1._2 + msg2._2, msg1._3)
    }

    val initGraph = graph.mapVertices((vId, attr) => if (vId == startVertex) EdmondsVertex(List(vId), 1, 0) else EdmondsVertex()).cache
    val result = initGraph.ops.pregel[(List[VertexId], Int, Int)]((List.empty, -1, -1))(
      applyMessages,
      sendMessage,
      mergeMessages
    )
    initGraph.unpersist(false)
    result
  }

  private def updateBC(bcVector: VertexRDD[Double], partialBc: VertexRDD[Double]) =
    bcVector.innerJoin(partialBc)((vId, bc1, bc2) => bc1 + bc2).cache()

  private def finalize(bcVector: VertexRDD[Double]) = {
    val result = BetweennessUtils.normalize(bcVector.mapValues(_ / 2), directed = false).cache
    result.count
    bcVector.unpersist(false)
    result
  }
}

object EdmondsBC extends Serializable {
  def computeBC[VD, ED: ClassTag](graph: Graph[VD, ED]) =
    new EdmondsBC[VD, ED](graph).computeBC
}

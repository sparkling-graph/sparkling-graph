package ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds

import ml.sparkling.graph.operators.algorithms.bfs.BFSShortestPath
import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.predicate.EdmondsBCPredicate
import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.processor.EdmondsBCProcessor
import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct.EdmondsVertex
import ml.sparkling.graph.operators.measures.vertex.betweenness.edmonds.struct.messages.EdmondsMessage
import ml.sparkling.graph.operators.utils.BetweennessUtils
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by mth on 3/12/17.
  */

class EdmondsBC[VD, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  lazy val simpleGraph = prepareRawGraph

  val bcAggregator = new EdmondsBCAggregator[ED]
  lazy val edmondsBFSProcessor = new BFSShortestPath[EdmondsVertex, ED, EdmondsMessage](new EdmondsBCPredicate, new EdmondsBCProcessor)

  private def prepareRawGraph = {
    graph.mapVertices((vId, attr) => EdmondsVertex())
  }

  def computeBC = {

    val verticesIds = simpleGraph.vertices.map({ case (vertexId, _) => vertexId }).cache
    val verticesIterator = verticesIds.toLocalIterator
    var betweennessVector: VertexRDD[Double] = simpleGraph.vertices.mapValues(_ => .0).cache()

    verticesIterator.foreach(processedVertex => {
      val bfsSP = edmondsBFSProcessor.computeSingleSelectedSourceBFS(simpleGraph, processedVertex)
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
    })

    verticesIds.unpersist(false)
    finalize(betweennessVector)
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

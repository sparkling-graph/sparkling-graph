package ml.sparkling.graph.operators.algorithms.bfs

import java.util.Date

import ml.sparkling.graph.operators.algorithms.bfs.predicate.BFSPredicate
import ml.sparkling.graph.operators.algorithms.bfs.processor.BFSProcessor
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
  * Created by mth on 3/13/17.
  */
class BFSShortestPath[VD: ClassTag, ED, MD: ClassTag](vPredicate: BFSPredicate[VD, MD], processor: BFSProcessor[VD, ED, MD]) extends Serializable {

  def computeSingleSelectedSourceBFS(graph: Graph[VD, ED], source: VertexId): Graph[VD, ED] = {
    val initGraph = graph.mapVertices((vId, attr) => vPredicate.getInitialData(vId, attr)(source)).cache

    val result = initGraph.ops.pregel[MD](processor.initialMessage)(
      vPredicate.applyMessages,
      processor.sendMessage,
      processor.mergeMessages
    )

    initGraph.unpersist(false)
    result
  }
}

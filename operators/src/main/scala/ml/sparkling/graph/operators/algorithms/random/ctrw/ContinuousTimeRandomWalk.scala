package ml.sparkling.graph.operators.algorithms.random.ctrw

import java.io.Serializable

import ml.sparkling.graph.operators.algorithms.pregel.Pregel
import ml.sparkling.graph.operators.algorithms.random.ctrw.factory.MessageFactory
import ml.sparkling.graph.operators.algorithms.random.ctrw.processor.CTRWProcessor
import ml.sparkling.graph.operators.algorithms.random.ctrw.struct.{CTRWMessage, CTRWVertex}
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}

import scala.reflect.ClassTag

/**
  * Created by mth
  */
class ContinuousTimeRandomWalk[VD, ED: ClassTag](graph: Graph[VD, ED], initTemp: Double = 2.3) extends Serializable {

  lazy val ctrwProcessor = new CTRWProcessor[VD, ED](graph, new MessageFactory(initTemp))

  def sampleVertices(sampleSize: Int = 1) = {

    val initGraph = ctrwProcessor.initGraph.mapVertices((id, v) => ctrwProcessor.createInitMessages(sampleSize))

    val resultGraph = Pregel[CTRWVertex, CTRWVertex, ED, List[CTRWMessage]](
      ctrwProcessor.initGraph,
      ctrwProcessor.createInitMessages(sampleSize),
      ctrwProcessor.applyMessages,
      ctrwProcessor.sendMessageCtx,
      ctrwProcessor.mergeMessages)

    val vertices = resultGraph.mapVertices((id, v) => v).vertices

    val res = vertices.flatMap({ case (vertexId, data) => data.messages.map(s => (s.src, vertexId))})
      .aggregateByKey(List[VertexId]())((l, s) => l :+ s, _ ++ _)

    res.checkpoint()
    res.count
    resultGraph.unpersistVertices(false)
    resultGraph.edges.unpersist(false)

    VertexRDD(res)
  }

}


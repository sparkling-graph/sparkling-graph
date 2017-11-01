package ml.sparkling.graph.operators.measures.vertex.betweenness.flow

import ml.sparkling.graph.operators.algorithms.random.ctrw.ContinuousTimeRandomWalk
import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.generator.FlowGenerator
import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.processor.CFBCProcessor
import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.struct.{CFBCFlow, CFBCVertex}
import org.apache.spark.graphx._

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by mth on 4/23/17.
  */
class CurrentFlowBC[VD, ED: ClassTag](graph: Graph[VD, ED], flowGenerator: FlowGenerator[CFBCVertex, Option[CFBCFlow]]) extends Serializable {

  lazy val cfbcProcessor = new CFBCProcessor[VD, ED](graph, flowGenerator)

  val k = flowGenerator.flowsPerVertex

  def computeBC(epsilon: Double) = {

    val ctrw = new ContinuousTimeRandomWalk[CFBCVertex, ED](cfbcProcessor.initGraph)
    val randomVertices = ctrw.sampleVertices(Math.ceil(Math.max(1, k / 10)) toInt)

    val initGraph = cfbcProcessor.initGraph.ops.joinVertices(randomVertices)((id, v, m) => {
      val sample = Random.shuffle(m.distinct.diff(List(id)))
      CFBCVertex(id, v.degree, v.bc, sample.toArray)

    })

    var i = 1

    var g1 = cfbcProcessor.createFlow(initGraph).cache

    var msg = cfbcProcessor.extractFlowMessages(g1).cache

    var msgCount = msg.filter({ case (id, m) => m.nonEmpty }).count
    var unfinalizedVertices = true

    while (msgCount > 0 || unfinalizedVertices) {
      val g2 = cfbcProcessor.preMessageExtraction(epsilon)(g1, msg).cache

      if (i % 20 == 0) { g2.checkpoint(); g2.vertices.count(); g2.edges.count() }

      val oldMsg = msg
      msg = cfbcProcessor.extractFlowMessages(g2).cache
      msgCount = msg.filter({ case (id, m) => m.nonEmpty }).count

      val g3 = cfbcProcessor.postMessageExtraction(g2).cache

      g1.unpersist(false)

      g1 = cfbcProcessor.createFlow(g3).cache
      g1.vertices.count
      g1.edges.count

      unfinalizedVertices = g1.vertices.aggregate(false)({ case (acc, (_, vData)) => acc || !vData.isFinalized(k) }, _ || _)

      g2.unpersist(false)
      g3.unpersist(false)
      oldMsg.unpersist(false)

      i = i + 1
    }

    initGraph.unpersist(false)

    g1.vertices.mapValues(v => v.bc)

  }

  def getNumberOfFlows(g: Graph[CFBCVertex, _]) = g.vertices.map({ case (_, v) => v.vertexFlows.length }).reduce(_ + _)

  def getNumberOfFetureFlows(g: Graph[CFBCVertex, _]) = g.vertices.map({ case (_, v) => v.availableSamples.length }).reduce(_ + _)

  def getNumOfActiveVertices(g: Graph[CFBCVertex, _]) = g.vertices.filter({ case (id, v) => v.isFinalized(k) } ).count()

}

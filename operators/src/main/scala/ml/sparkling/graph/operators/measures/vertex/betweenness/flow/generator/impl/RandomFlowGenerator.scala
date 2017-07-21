package ml.sparkling.graph.operators.measures.vertex.betweenness.flow.generator.impl

import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.factory.FlowFactory
import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.generator.FlowGenerator
import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.struct.{CFBCFlow, CFBCVertex}

import scala.util.Random

/**
  * Created by mth on 4/28/17.
  */
class RandomFlowGenerator(val phi: Int, n: Int, k: Int, factory: FlowFactory[CFBCVertex, CFBCFlow]) extends FlowGenerator[CFBCVertex, Option[CFBCFlow]] {
  override def createFlow(arg: CFBCVertex): Option[CFBCFlow] =
    if (shouldGenerateFlow(arg)) Some(factory.create(arg)) else None


  def shouldGenerateFlow(vertex: CFBCVertex) = {
    val p = Math.max(0.0, (phi.toDouble - vertex.vertexPhi.toDouble) / n.toDouble)
    val r = Random.nextDouble()
    r <= p && !vertex.isFinalized(k)
  }

  override def flowsPerVertex = k
}

package ml.sparkling.graph.operators.measures.vertex.betweenness.flow.factory.impl

import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.factory.FlowFactory
import ml.sparkling.graph.operators.measures.vertex.betweenness.flow.struct.{CFBCFlow, CFBCVertex}

import scala.util.Random

/**
  * Created by mth on 4/23/17.
  */
class DefaultFlowFactory extends FlowFactory[CFBCVertex, CFBCFlow] {

  val initPotential = 1.0d

  override def create(arg: CFBCVertex): CFBCFlow = {
    val dst = arg.availableSamples(Random.nextInt(arg.availableSamples.length))
    CFBCFlow(arg.id, dst, initPotential)
  }
}

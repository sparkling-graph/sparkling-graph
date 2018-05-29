package ml.sparkling.graph.operators.partitioning

import java.util.Random

import ml.sparkling.graph.operators.partitioning.PropagationBasedPartitioning.VertexOperator

/**
 * Created by  Roman Bartusiak <riomus@gmail.com> on 24.04.18.
 */
object RandomVertexOperator extends VertexOperator {
  def apply(v1: Long, v2: Long):Long = {
    if (new Random(v1+v2).nextDouble() < 0.5){
      v1
    }else{
      v2
    }
  }
}

package ml.sparkling.graph.operators.algorithms.aproximation

import ml.sparkling.graph.api.operators.IterativeComputation.{SimpleVertexPredicate, VertexPredicate}
import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.Component
import org.apache.spark.graphx._

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 07.02.17.
  */
case class AnyMatchingComponentPredicate(predicate:SimpleVertexPredicate) extends VertexPredicate[Component] with Serializable{

  override def apply[B<:Component](id:VertexId,data:B):Boolean=data.exists(predicate(_))

}

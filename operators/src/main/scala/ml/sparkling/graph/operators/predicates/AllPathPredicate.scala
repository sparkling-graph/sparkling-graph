package ml.sparkling.graph.operators.predicates

import ml.sparkling.graph.api.operators.IterativeComputation.{SimpleVertexPredicate, VertexPredicate}
import org.apache.spark.graphx.VertexId

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Always true predicate
 */
object AllPathPredicate extends VertexPredicate[Any] with Serializable with SimpleVertexPredicate {
  override def apply[B<:Any](v1: VertexId, v2: B): Boolean = true

  override def apply(id: VertexId): Boolean = true
}

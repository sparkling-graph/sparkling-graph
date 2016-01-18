package sparkling.graph.operators.predicates

import org.apache.spark.graphx.VertexId
import sparkling.graph.api.operators.IterativeComputation.VertexPredicate

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Always true predicate
 */
object AllPathPredicate extends VertexPredicate with Serializable {
  override def apply(v1: VertexId): Boolean = true
}

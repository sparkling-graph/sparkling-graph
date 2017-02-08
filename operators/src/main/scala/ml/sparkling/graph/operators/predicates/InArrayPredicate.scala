package ml.sparkling.graph.operators.predicates

import ml.sparkling.graph.api.operators.IterativeComputation.{SimpleVertexPredicate, VertexPredicate}
import org.apache.spark.graphx.VertexId

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Predicate for single vertexId
 */
case class InArrayPredicate(vertices:Array[VertexId]) extends VertexPredicate[Any] with Serializable with SimpleVertexPredicate {

  override def apply[B<:Any](id:VertexId,data:B):Boolean=apply(id)
  override def apply(v1: VertexId): Boolean = vertices.contains(v1)

}

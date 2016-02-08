package ml.sparkling.graph.api.operators

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object IterativeComputation {

  type VertexPredicate = VertexId => Boolean
  type BucketSizeProvider[VD, ED] = Graph[VD, ED] => Long

  def wholeGraphBucket[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]) = {
    graph.numVertices
  }
}

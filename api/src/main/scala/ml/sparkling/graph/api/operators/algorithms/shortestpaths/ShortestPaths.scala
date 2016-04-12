package ml.sparkling.graph.api.operators.algorithms.shortestpaths

import ml.sparkling.graph.api.operators.IterativeComputation
import ml.sparkling.graph.api.operators.IterativeComputation.VertexPredicate
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes._
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
trait ShortestPaths {
  /**
   * Compute all shortest paths preserving them in vertices
   * @param graph - graph for computation
   * @param treatAsUndirected - treat graph as undirected (each path will be bidirectional)
   * @param num  - numeric to operate on  edge lengths
   * @tparam VD - vertex type
   * @tparam ED -  edge type
   * @return - Graph where each vertex contains all its shortest paths, with structure of them
   */
  def compute[VD, ED:ClassTag](graph: Graph[VD, ED], vertexPredicate:VertexPredicate, treatAsUndirected: Boolean = false)(implicit num: Numeric[ED]): Graph[WithPathContainer, ED]
}

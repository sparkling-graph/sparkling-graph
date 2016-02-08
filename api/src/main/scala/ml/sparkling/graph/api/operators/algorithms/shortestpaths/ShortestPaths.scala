package ml.sparkling.graph.api.operators.algorithms.shortestpaths

import ml.sparkling.graph.api.operators.IterativeComputation
import org.apache.spark.graphx.Graph
import IterativeComputation.VertexPredicate
import ShortestPathsTypes._

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
  def computeShortestPaths[VD, ED:ClassTag](graph: Graph[VD, ED], vertexPredicate:VertexPredicate, treatAsUndirected: Boolean = false)(implicit num: Numeric[ED]): Graph[WithPathContainer, ED]
}

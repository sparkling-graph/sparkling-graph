package ml.sparkling.graph.examples

import java.util

import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes.{JDouble, JLong}
import ml.sparkling.graph.operators.algorithms.aproximation.ApproximatedShortestPathsAlgorithm
import ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation.LPCoarsening
import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance
import ml.sparkling.graph.operators.predicates.ByIdsPredicate
import org.apache.log4j.Logger
import org.apache.spark.graphx.Graph

import scala.collection.JavaConversions._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
case object ApproximateShortestPathsToDirectory extends ExampleApp {


  def body() = {
    ApproximatedShortestPathsAlgorithm.computeAPSPToDirectory(partitionedGraph, out, treatAsUndirected,bucketSize)
    ctx.stop()
  }


}


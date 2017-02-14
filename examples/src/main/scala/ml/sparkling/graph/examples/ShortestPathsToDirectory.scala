package ml.sparkling.graph.examples


import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
case object ShortestPathsToDirectory extends ExampleApp {

  def body() = {
    ShortestPathsAlgorithm.computeAPSPToDirectory(partitionedGraph, out, treatAsUndirected,bucketSize)
    ctx.stop()
  }


}


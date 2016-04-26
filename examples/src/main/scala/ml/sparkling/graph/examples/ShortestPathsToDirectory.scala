package ml.sparkling.graph.examples

import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance
import ml.sparkling.graph.operators.predicates.ByIdsPredicate

import scala.collection.JavaConversions._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object ShortestPathsToDirectory extends ExampleApp {
  def body() = {
    val verticesGroups = partitionedGraph.vertices.map(_._1).sortBy(k=>k).collect().grouped(bucketSize.toInt)
    (verticesGroups).foreach(group => {
      val shortestPaths = ShortestPathsAlgorithm.computeShortestPathsLengths(partitionedGraph, new ByIdsPredicate(group.toList),treatAsUndirected )
      val joinedGraph = partitionedGraph
        .outerJoinVertices(shortestPaths.vertices)((vId, data, newData) => ( data, newData.getOrElse(new FastUtilWithDistance.DataMap)))
      joinedGraph.vertices.values.map{
        case (vertex,data) => {
          val entries = data.entrySet().toList.sortBy(_.getKey)
          val distancesInString=entries.map(e=>s"${e.getKey}:${e.getValue.toInt}")
          (vertex :: distancesInString ).mkString(";")
        }
      }.saveAsTextFile(s"${out}/from_${group.head}")
    })

    partitionedGraph.vertices.map(t => List(t._1, t._2).mkString(";")).saveAsTextFile(s"${out}/index")
    ctx.stop()
  }
}


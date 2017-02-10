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
object ApproximateShortestPathsToDirectory extends ExampleApp {

  val logger=Logger.getLogger(ApproximateShortestPathsToDirectory.getClass())

  def body() = {
    computeAPSPToDirectory(partitionedGraph, out, treatAsUndirected,bucketSize)
    ctx.stop()
  }

  def computeAPSPToDirectory(graph: Graph[String, Double], outDirectory: String, treatAsUndirected: Boolean, bucketSize:Long): Unit = {
    val coarsedGraph=LPCoarsening.coarse(graph,treatAsUndirected)
    logger.info(s"Coarsed graph has size ${coarsedGraph.vertices.count()} in comparision to ${graph.vertices.count()}")
    val verticesGroups = graph.vertices.map(_._1).sortBy(k => k).collect().grouped(bucketSize.toInt)
    (verticesGroups).foreach(group => {

      val shortestPaths = ApproximatedShortestPathsAlgorithm.computeShortestPathsLengthsWithoutCoarsingUsing(graph,coarsedGraph, new ByIdsPredicate(group.toSet), treatAsUndirected)
      val joinedGraph = graph
        .outerJoinVertices(shortestPaths.vertices)((vId, data, newData) => (data, newData.getOrElse(new FastUtilWithDistance.DataMap)))
      joinedGraph.vertices.values.map {
        case (vertex, data: util.Map[JLong, JDouble]) => {
          val dataStr = data.entrySet()
            .map(e=>s"${e.getKey}:${e.getValue}").mkString(";")
          s"$vertex;$dataStr"
        }
      }.saveAsTextFile(s"${outDirectory}/from_${group.head}")
    })

    graph.vertices.map(t => List(t._1, t._2).mkString(";")).saveAsTextFile(s"${outDirectory}/index")
  }
}


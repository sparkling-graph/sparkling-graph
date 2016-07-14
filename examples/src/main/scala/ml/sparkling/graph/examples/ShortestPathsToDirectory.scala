package ml.sparkling.graph.examples

import java.util

import breeze.linalg.VectorBuilder
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes.{JDouble, JLong}
import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance
import ml.sparkling.graph.operators.predicates.ByIdsPredicate
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object ShortestPathsToDirectory extends ExampleApp {

  def body() = {
    computeAPSPToDirectory(partitionedGraph, out, treatAsUndirected,bucketSize)
    ctx.stop()
  }

  def computeAPSPToDirectory(graph: Graph[String, Double], outDirectory: String, treatAsUndirected: Boolean, bucketSize:Long): Unit = {
    val verticesGroups = graph.vertices.map(_._1).sortBy(k => k).collect().grouped(bucketSize.toInt)
    (verticesGroups).foreach(group => {

      val shortestPaths = ShortestPathsAlgorithm.computeShortestPathsLengths(graph, new ByIdsPredicate(group.toList), treatAsUndirected)
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


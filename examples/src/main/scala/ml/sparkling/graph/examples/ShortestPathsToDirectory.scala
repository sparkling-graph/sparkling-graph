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
    val vertexCount = partitionedGraph.numVertices
    val broadcastBucketSize = ctx.broadcast(bucketSize)
    val broadcastVertexCount = ctx.broadcast(vertexCount)
    (1l until vertexCount + 1 by bucketSize).foreach(startVid => {
      val broadcastStartVid = ctx.broadcast(startVid)
      val shortestPaths = ShortestPathsAlgorithm.computeShortestPathsLengths(partitionedGraph, new ByIdsPredicate((startVid until startVid + bucketSize).toList))
      val joinedGraph = partitionedGraph
        .outerJoinVertices(shortestPaths.vertices)((vId, data, newData) => (vId, data, newData.getOrElse(new FastUtilWithDistance.DataMap)))
      joinedGraph.vertices.values.map(data => {
        var entries = data._3
          .entrySet().toList.sortBy(_.getKey)
        var a = 0l
        val stringBuilder = new StringBuilder
        while (a < broadcastBucketSize.value && (a+broadcastStartVid.value)<broadcastVertexCount.value) {
          if (entries.size > 0 && a == entries.head.getKey) {
            stringBuilder ++= s"${entries.head.getValue.toInt},"
            entries = entries.drop(1)
          }
          else {
            stringBuilder ++= "0,"
          }
          a += 1l
        }
        stringBuilder.setLength(stringBuilder.length - 1)
        s"${data._1},${stringBuilder}"
      }).saveAsTextFile(s"${out}/from_${startVid}")
    })

    partitionedGraph.vertices.map(t => List(t._1, t._2).mkString(",")).saveAsTextFile(s"${out}/index")
    ctx.stop()
  }
}


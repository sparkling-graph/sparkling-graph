package ml.sparkling.graph.examples

import breeze.linalg.VectorBuilder
import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance
import ml.sparkling.graph.operators.predicates.ByIdsPredicate
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object ShortestPathsToDirectory extends ExampleApp {

  def body() = {
    val verticesGroups = partitionedGraph.vertices.map(_._1).sortBy(k=>k).collect().grouped(bucketSize.toInt)
    (verticesGroups).foreach(group => {
      val dataSpread=Math.min(partitionedGraph.numVertices.toInt,Math.max(bucketSize.toInt,group.last.toInt-group.head.toInt))
      val shortestPaths = ShortestPathsAlgorithm.computeShortestPathsLengths(partitionedGraph, new ByIdsPredicate(group.toList),treatAsUndirected )
      val joinedGraph = partitionedGraph
        .outerJoinVertices(shortestPaths.vertices)((vId, data, newData) => ( data, newData.getOrElse(new FastUtilWithDistance.DataMap)))
      joinedGraph.vertices.values.map{
        case (vertex,data) => {
          val dataStr=data.entrySet()
            .foldLeft(new VectorBuilder[Int](dataSpread))((b, e)=>{b.add(e.getKey.toInt-group.head.toInt,e.getValue.toInt);b})
            .toDenseVector.toArray.mkString(";")
         s"$vertex;$dataStr"
        }
      }.saveAsTextFile(s"${out}/from_${group.head}")
    })

    partitionedGraph.vertices.map(t => List(t._1, t._2).mkString(";")).saveAsTextFile(s"${out}/index")
    ctx.stop()
  }
}


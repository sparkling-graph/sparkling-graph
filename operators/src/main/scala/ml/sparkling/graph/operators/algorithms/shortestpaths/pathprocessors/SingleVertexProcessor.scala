package ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes
import org.apache.spark.graphx.VertexId
import ShortestPathsTypes._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Path processors that computes shortest paths lengths using standard scala collections
 */
class SingleVertexProcessor[VD, ED](computedVertexId:VertexId) extends PathProcessor[VD, ED, Double] {
  def EMPTY_CONTAINER = 0d

  override def getNewContainerForPaths(): Double = 0d

  override def extendPaths(targetVertexId: VertexId, currentValue: Double, vertexId: VertexId, distance: ED)(implicit num: Numeric[ED]): Double = {
    if(targetVertexId==computedVertexId || currentValue == 0)
      0d
    else
      currentValue+num.toDouble(distance)
  }

  override def mergePathContainers(map1: Double, map2: Double)(implicit num: Numeric[ED]): Double = {
    (map1,map2) match{
      case (0d,_)=> map2
      case (_,0d)=> map1
      case _ =>Math.min(map1,map2)
    }
  }

  override def putNewPath(map: Double, to: VertexId, weight: ED)(implicit num: Numeric[ED]): Double = {
   num.toDouble(weight)
  }

}
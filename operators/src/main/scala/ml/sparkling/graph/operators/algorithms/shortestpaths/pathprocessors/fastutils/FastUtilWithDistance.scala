package ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils

import it.unimi.dsi.fastutil.longs._
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes._
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.PathProcessor
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance.DataMap
import org.apache.spark.graphx.VertexId

import scala.collection.JavaConversions._
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Path processor that utilizes it.unimi.dsi.fastutil as data store, and computes only distances
 */
class FastUtilWithDistance[VD, ED]() extends PathProcessor[VD, ED, DataMap] {
  def EMPTY_CONTAINER = getNewContainerForPaths()
  def getNewContainerForPaths() = {
   new DataMap()
  }

  def putNewPath(map: DataMap, to: VertexId, weight: ED)(implicit num: Numeric[ED]): DataMap = {
    val out=map.asInstanceOf[DataMap].clone()
    out.put(to, num.toDouble(weight))
    out
  }

  def mergePathContainers(map1: DataMap, map2: DataMap)(implicit num: Numeric[ED]) = {
    val out=map1.clone()
    map2.foreach{case (key,inValue)=>{
      val map1Value: JDouble =Option(map1.get(key)).getOrElse(inValue)
      val map2Value: JDouble =  inValue
      val value: JDouble = java.lang.Double.min(map1Value, map2Value);
      out.put(key, value)
    }}
    out
  }


  def extendPaths(targetVertexId:VertexId,map: DataMap, vertexId: VertexId, distance: ED)(implicit num: Numeric[ED]):DataMap = {
    val out=map.clone()
    val toAdd=num.toDouble(distance)
    map.keySet().foreach{ (key: JLong) => {
        out.addTo(key, toAdd)
    }}
    out.remove(targetVertexId)
    out
  }

}

object FastUtilWithDistance{
  type DataMap=Long2DoubleOpenHashMap
}
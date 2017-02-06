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
class FastUtilWithDistance[VD, ED]() extends PathProcessor[VD, ED, JMap[JLong, JDouble]] {
  def EMPTY_CONTAINER = getNewContainerForPaths()
  def getNewContainerForPaths() = {
   new DataMap()
  }

  def putNewPath(map: JMap[JLong, JDouble], to: VertexId, weight: ED)(implicit num: Numeric[ED]): JMap[JLong, JDouble] = {
    val out=map.asInstanceOf[DataMap].clone()
    out.put(to, num.toDouble(weight))
    out
  }

  def mergePathContainers(map1: JMap[JLong, JDouble], map2: JMap[JLong, JDouble])(implicit num: Numeric[ED]) = {
    val typedMap1=map1.asInstanceOf[DataMap]
    val typedMap2=map2.asInstanceOf[DataMap]
    val out=typedMap1.clone()
    typedMap2.foreach{case (key,inValue)=>{
      val map1Value: JDouble =Option(typedMap1.get(key)).getOrElse(inValue)
      val map2Value: JDouble =  inValue
      val value: JDouble = java.lang.Double.min(map1Value, map2Value);
      out.put(key, value)
    }}
    out
  }


  def extendPaths(targetVertexId:VertexId,map: JMap[JLong, JDouble], vertexId: VertexId, distance: ED)(implicit num: Numeric[ED]): JMap[JLong, JDouble] = {
    val out=map.asInstanceOf[DataMap].clone()
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
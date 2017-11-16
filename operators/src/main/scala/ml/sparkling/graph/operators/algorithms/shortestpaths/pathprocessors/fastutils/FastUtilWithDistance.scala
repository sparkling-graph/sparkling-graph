package ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils

import it.unimi.dsi.fastutil.longs._
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes._
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.PathProcessor
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance.DataMap
import ml.sparkling.graph.operators.utils.LoggerHolder
import org.apache.spark.graphx.VertexId

import scala.collection.JavaConversions._


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Path processor that utilizes it.unimi.dsi.fastutil as data store, and computes only distances
 */
class FastUtilWithDistance[VD, ED]() extends PathProcessor[VD, ED, DataMap] {
  def EMPTY_CONTAINER = new DataMap(0)
  def getNewContainerForPaths() = {
   new DataMap(64,0.25f)
  }

  def putNewPath(map: DataMap, to: VertexId, weight: ED)(implicit num: Numeric[ED]): DataMap = {
    val out=map.asInstanceOf[DataMap].clone()
    out.put(to, num.toDouble(weight))
    out
  }

  def processNewMessages(map1: DataMap, map2: DataMap)(implicit num: Numeric[ED]):DataMap = {
    mergeMessages(map1,map2.clone())
  }

  override def mergeMessages(map1: DataMap, map2: DataMap)(implicit num: Numeric[ED]):DataMap = {
    val out=map2
    map1.foreach{case (key: JLong,inValue: JDouble)=>{
      val longKey=key.toLong
      val value: Double =if(map2.containsKey(longKey)) {
        min(inValue,map2.get(key.toLong))
      }else{
        inValue
      }
      out.put(longKey, value)
    }}
    out
  }

  def min(d1:JDouble,d2:JDouble):JDouble={
    if(d1<d2){
      d1
    }else{
      d2
    }
  }

  def extendPathsMerging(targetVertexId:VertexId,map: DataMap, vertexId: VertexId, distance: ED,map2: DataMap)(implicit num: Numeric[ED]):DataMap = {
    val out=map2.clone()
    val toAdd=num.toDouble(distance)
    map.foreach{case (key: JLong,inValue: JDouble)=>{
      if(!targetVertexId.equals(key)){
        val longKey=key.toLong
        val value: Double =if(map2.containsKey(longKey)) {
          min(inValue+toAdd,map2.get(longKey))
        }else{
          inValue+toAdd
        }
        out.put(longKey,value)
      }
    }}
    out
  }

}

object FastUtilWithDistance{
  type DataMap=Long2DoubleOpenHashMap
}
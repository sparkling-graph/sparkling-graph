package ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils

import java.util.function.{Consumer, BiConsumer}

import it.unimi.dsi.fastutil.longs._
import it.unimi.dsi.fastutil.objects._
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.PathProcessor
import org.apache.spark.graphx.VertexId
import ShortestPathsTypes._


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Path processor that utilizes it.unimi.dsi.fastutil as data store, and computes all paths with their structure
 */
class FastUtilWithPath[VD,ED]() extends  PathProcessor[VD,ED,WithPathContainer]{
  private type PathsSet=ObjectOpenHashSet[JPath]
  private type PathsMap=Long2ObjectOpenHashMap[JPath]
  private type SinglePath=ObjectArrayList[JDouble]
  def EMPTY_CONTAINER=getNewContainerForPaths(0)

  def getNewContainerForPaths() ={
    new PathsMap().asInstanceOf[WithPathContainer]
  }

  def getNewContainerForPaths(size:Int) ={
    new PathsMap(size,1).asInstanceOf[WithPathContainer]
  }

  def putNewPath(map:WithPathContainer,to:VertexId,weight:ED)(implicit num:Numeric[ED]): WithPathContainer={
    val existingPaths =getPathsContainer()
    val newPath=new SinglePath()
    newPath.add(num.toDouble(weight))
    existingPaths.add(newPath)
    map.put(to,existingPaths)
    map
  }

  def mergePathContainers(map1:WithPathContainer,map2:WithPathContainer)(implicit num:Numeric[ED]):WithPathContainer={
    val out=map1.asInstanceOf[PathsMap].clone().asInstanceOf[WithPathContainer]
    map2.forEach(new BiConsumer[JLong,JPathCollection](){
      def accept(key: JLong, u: JPathCollection) = {
        val map1Value: JPathCollection =Option(map1.get(key)).getOrElse(ObjectSets.EMPTY_SET.asInstanceOf[JPathCollection])
        val map2Value: JPathCollection =u
        val value=mergePathSets(map1Value,map2Value)
        out.put(key,value)
      }
    })
    out
  }


  def extendPaths(targetVertexId:VertexId,map:WithPathContainer,vertexId:VertexId,distance:ED)(implicit num:Numeric[ED]): WithPathContainer ={
    val out=getNewContainerForPaths(map.size())
    map.forEach(new BiConsumer[JLong,JPathCollection](){
      def accept(k: JLong, u: JPathCollection) = {
        if (!targetVertexId.equals(k)) {
          out.put(k, extendPathsSet(map.get(k), vertexId, distance))
        } else {
          out.remove(k)
        }
      }
    })
    out
  }

  private def extendPathsSet(set:JPathCollection,vertexId:VertexId,distance:ED)(implicit num:Numeric[ED]):JPathCollection={
    val out =getPathsContainer(set.size())
    set.forEach(new Consumer[JPath](){
      def accept( l: JPath) = {
        val lClone=l.asInstanceOf[SinglePath].clone()
        lClone.add(vertexId.toDouble)
        lClone.set(0,lClone.get(0)+num.toDouble(distance))
        out.add(lClone)
      }
    })
    out
  }

  private def mergePathSets(set1:JPathCollection,set2:JPathCollection)(implicit num:Numeric[ED]): JPathCollection ={
    val firstSetLength=if(set1.size()==0) {java.lang.Double.MAX_VALUE.asInstanceOf[JDouble]} else {set1.iterator().next().get(0)}
    val secondSetLength=if(set2.size()==0) {java.lang.Double.MAX_VALUE.asInstanceOf[JDouble]} else { set2.iterator().next().get(0)}
    if(firstSetLength>secondSetLength){
      set2
    }else if(firstSetLength<secondSetLength){
      set1
    }else{
      val set1Clone= set1.asInstanceOf[PathsSet].clone()
      set1Clone.addAll(set2)
      set1Clone
    }
  }
  private def getPathsContainer(size:Int=50): JPathCollection ={
    new PathsSet(size,1)
  }
}
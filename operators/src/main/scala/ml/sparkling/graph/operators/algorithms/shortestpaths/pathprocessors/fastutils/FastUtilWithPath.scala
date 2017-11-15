package ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils

import java.util.function.{BiConsumer, Consumer}

import it.unimi.dsi.fastutil.longs._
import it.unimi.dsi.fastutil.objects._
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes._
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.PathProcessor
import org.apache.spark.graphx.VertexId


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Path processor that utilizes it.unimi.dsi.fastutil as data store, and computes all paths with their structure
 */
class FastUtilWithPath[VD,ED]() extends  PathProcessor[VD,ED,WithPathContainer]{
  private type PathsSet=ObjectOpenHashSet[JPath]
  private type PathsMap=Long2ObjectOpenHashMap[JPath]
  private type SinglePath=ObjectArrayList[JDouble]
  private val DEFAULT_CONTAINER_SIZE=64
  def EMPTY_CONTAINER=getNewContainerForPaths(0)

  def getNewContainerForPaths() ={
    new PathsMap(DEFAULT_CONTAINER_SIZE,0.25f).asInstanceOf[WithPathContainer]
  }

  def getNewContainerForPaths(size:Int) ={
    new PathsMap(size,0.25f).asInstanceOf[WithPathContainer]
  }

  def putNewPath(map:WithPathContainer,to:VertexId,weight:ED)(implicit num:Numeric[ED]): WithPathContainer={
    val existingPaths =getPathsContainer()
    val newPath=new SinglePath()
    newPath.add(num.toDouble(weight))
    existingPaths.add(newPath)
    val out=map.asInstanceOf[PathsMap].clone().asInstanceOf[WithPathContainer];
    out.put(to,existingPaths)
    out
  }

  def processNewMessages(map1:WithPathContainer,map2:WithPathContainer)(implicit num:Numeric[ED]):WithPathContainer={
    val out=map2.asInstanceOf[PathsMap].clone().asInstanceOf[WithPathContainer]
    mergeMessages(map1,out)
  }

  override def mergeMessages(map1:WithPathContainer, map2:WithPathContainer)(implicit num:Numeric[ED]):WithPathContainer={
    val out=map2
    map1.forEach(new BiConsumer[JLong,JPathCollection](){
      def accept(key: JLong, u: JPathCollection) = {
        val map2Value: JPathCollection =Option(map2.get(key)).getOrElse(ObjectSets.EMPTY_SET.asInstanceOf[JPathCollection])
        val map1Value: JPathCollection =u
        val value=mergePathSets(map1Value,map2Value)
        out.put(key,value)
      }
    })
    out
  }


  def extendPathsMerging(targetVertexId:VertexId,map:WithPathContainer,vertexId:VertexId,distance:ED,map2:WithPathContainer)(implicit num:Numeric[ED]): WithPathContainer ={
    val out=map2.asInstanceOf[PathsMap].clone().asInstanceOf[WithPathContainer]
    map.forEach(new BiConsumer[JLong,JPathCollection](){
      def accept(k: JLong, u: JPathCollection) = {
        if (!targetVertexId.equals(k)) {
          val map2Value: JPathCollection =Option(map2.get(k)).getOrElse(ObjectSets.EMPTY_SET.asInstanceOf[JPathCollection])
          val coll=extendPathsSet(targetVertexId,map.get(k), vertexId, distance)
          val value=mergePathSets(coll,map2Value)
          out.put(k,value)
        }
      }
    })
    out
  }

  private def extendPathsSet(targetVertexId:VertexId,set:JPathCollection,vertexId:VertexId,distance:ED)(implicit num:Numeric[ED]):JPathCollection={
    val out =getPathsContainer(set.size())
    val javaTarget:JDouble=targetVertexId.toDouble;
    set.forEach(new Consumer[JPath](){
      def accept( l: JPath) = {
        if(l.indexOf(javaTarget)<1){
          val lClone=l.asInstanceOf[SinglePath].clone()
          lClone.add(vertexId.toDouble)
          lClone.set(0,lClone.get(0)+num.toDouble(distance))
          out.add(lClone)
        }
      }
    })
    out
  }

  private def mergePathSets(set1:JPathCollection,set2:JPathCollection)(implicit num:Numeric[ED]): JPathCollection ={
    val firstSetLength=if(set1.size()==0) {java.lang.Double.MAX_VALUE.asInstanceOf[JDouble]} else {set1.iterator().next().get(0)}
    val secondSetLength=if(set2.size()==0) {java.lang.Double.MAX_VALUE.asInstanceOf[JDouble]} else { set2.iterator().next().get(0)}
    firstSetLength compareTo secondSetLength signum match{
      case 0=>{
        val set1Clone= set1.asInstanceOf[PathsSet].clone()
        set1Clone.addAll(set2)
        set1Clone
      }
      case 1 => set2.asInstanceOf[PathsSet].clone()
      case -1 => set1.asInstanceOf[PathsSet].clone()
    }
  }
  private def getPathsContainer(size:Int=DEFAULT_CONTAINER_SIZE): JPathCollection ={
    new PathsSet(size,1)
  }
}
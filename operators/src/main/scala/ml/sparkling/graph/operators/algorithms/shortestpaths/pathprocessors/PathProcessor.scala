package ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors

import org.apache.spark.graphx.VertexId

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
trait PathProcessor[VD,ED,PS] extends Serializable{
  def EMPTY_CONTAINER:PS
  def getNewContainerForPaths():PS
  def putNewPath(map:PS,to:VertexId,weight:ED)(implicit num:Numeric[ED]): PS
  def processNewMessages(map1:PS, map2:PS)(implicit num:Numeric[ED]):PS
  def mergeMessages(map1:PS, map2:PS)(implicit num:Numeric[ED]):PS={
    processNewMessages(map1,map2)
  }
  def extendPathsMerging(targetVertexId:VertexId,map:PS,vertexId:VertexId,distance:ED,map2:PS)(implicit num:Numeric[ED]): PS
}

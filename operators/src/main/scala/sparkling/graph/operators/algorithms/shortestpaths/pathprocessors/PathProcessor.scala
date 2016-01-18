package sparkling.graph.operators.algorithms.shortestpaths.pathprocessors

import it.unimi.dsi.fastutil.longs._
import org.apache.spark.graphx.{EdgeContext, VertexId}
import sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes
import ShortestPathsTypes._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
trait PathProcessor[VD,ED,PS] extends Serializable{
  def EMPTY_CONTAINER:PS
  def getNewContainerForPaths():PS
  def putNewPath(map:PS,to:VertexId,weight:ED)(implicit num:Numeric[ED]): PS
  def mergePathContainers(map1:PS,map2:PS)(implicit num:Numeric[ED]):PS
  def extendPaths(targetVertexId:VertexId,map:PS,vertexId:VertexId,distance:ED)(implicit num:Numeric[ED]): PS
}

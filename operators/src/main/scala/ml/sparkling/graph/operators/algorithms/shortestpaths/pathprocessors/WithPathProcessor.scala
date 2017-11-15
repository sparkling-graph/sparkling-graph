package ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors

import org.apache.spark.graphx.VertexId


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Path processors that computes shortest paths with their structures using standard scala collections
 */
class WithPathProcessor[VD,ED]() extends  PathProcessor[VD,ED,Map[VertexId,(ED,Set[List[VertexId]])]]{
  private type PathsSet=(ED,Set[List[VertexId]])
  private type PathsMap=Map[VertexId,PathsSet]
  def EMPTY_CONTAINER=Map.empty[VertexId,PathsSet]

  def getNewContainerForPaths() ={
    EMPTY_CONTAINER
  }

  def putNewPath(map:PathsMap,to:VertexId,weight:ED)(implicit num:Numeric[ED]): PathsMap={
    (map + (to -> (weight,Set(to::Nil)))).map(identity)
  }

  def processNewMessages(map1:PathsMap,map2:PathsMap)(implicit num:Numeric[ED]):PathsMap={
    (map1.keySet ++ map2.keySet).map(vId=>(vId,mergePathSets(map1.get(vId),map2.get(vId)))).toMap.map(identity)
  }


  def extendPathsMerging(targetVertexId:VertexId,map:PathsMap,vertexId:VertexId,distance:ED,map2:PathsMap)(implicit num:Numeric[ED]): PathsMap ={
    val extended=map.filterKeys(_!=targetVertexId).mapValues(extendPathsSet(_,vertexId,distance)).map(identity)
    processNewMessages(extended,map2)
  }

  private def extendPathsSet(pathSet:PathsSet,vertexId:VertexId,distance:ED)(implicit num:Numeric[ED]):PathsSet={
    pathSet match{
      case (edge,set) =>  (num.plus(distance,edge),set.map(vertexId :: _))
    }

  }

  private def mergePathSets(pathSet1:Option[PathsSet],pathSet2:Option[PathsSet])(implicit num:Numeric[ED]): PathsSet ={
    (pathSet1 :: pathSet2 :: Nil).flatten[PathsSet].reduce[PathsSet]{
      case ((edge1,set1),(edge2,set2))=>
        num.compare(edge1,edge2).signum match{
          case 0=> (edge1,set1++set2)
          case 1=>(edge2,set2)
          case -1=>(edge1,set1)
        }
    }
  }
}
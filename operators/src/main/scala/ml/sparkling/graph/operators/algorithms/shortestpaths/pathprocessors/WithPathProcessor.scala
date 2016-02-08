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

  def mergePathContainers(map1:PathsMap,map2:PathsMap)(implicit num:Numeric[ED]):PathsMap={
    (map1.keySet ++ map2.keySet).map(vId=>(vId,mergePathSets(map1.get(vId),map2.get(vId)))).toMap.map(identity)
  }


  def extendPaths(targetVertexId:VertexId,map:PathsMap,vertexId:VertexId,distance:ED)(implicit num:Numeric[ED]): PathsMap ={
   map.filterKeys(_!=targetVertexId).mapValues(extendPathsSet(_,vertexId,distance)).map(identity)
  }

  private def extendPathsSet(set:PathsSet,vertexId:VertexId,distance:ED)(implicit num:Numeric[ED]):PathsSet={
    (num.plus(distance,set._1),set._2.map(vertexId :: _))
  }

  private def mergePathSets(set1:Option[PathsSet],set2:Option[PathsSet])(implicit num:Numeric[ED]): PathsSet ={
    (set1 :: set2 :: Nil).flatten.reduce((a,b)=>{
      if(num.gt(a._1,b._1)){
        b
      }else if(num.gt(b._1,a._1)){
        a
      }else{
        (a._1,a._2++b._2)
      }
    })
  }
}
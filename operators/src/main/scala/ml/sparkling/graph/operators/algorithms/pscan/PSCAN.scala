package ml.sparkling.graph.operators.algorithms.pscan

import ml.sparkling.graph.operators.measures.utils.CollectionsUtils._
import ml.sparkling.graph.operators.measures.utils.NeighboursUtils
import ml.sparkling.graph.operators.measures.utils.NeighboursUtils.NeighbourSet
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag
import scala.util.Try

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object PSCAN {

  implicit class DSL[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED]){
    def PSCAN(epsilon:Double=0.1):Graph[ComponentID,ED]={
      computeConnectedComponents(graph,epsilon)
    }
  }
  

  type ComponentID=Long


  def computeConnectedComponents[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],epsilon:Double=0.1):Graph[ComponentID,ED]={

    val neighbours: Graph[NeighbourSet, ED] = NeighboursUtils.getWithNeighbours(graph,treatAsUndirected = true)

    val edgesWithSimilarity=neighbours.mapTriplets(edge=>{
      val sizeOfIntersection=intersectSize(edge.srcAttr,edge.dstAttr)
      val denominator = Math.sqrt(edge.srcAttr.size()*edge.dstAttr.size())
      sizeOfIntersection/denominator
    })

    val cutOffGraph=edgesWithSimilarity.filter[NeighbourSet, Double](
      preprocess=g=>g,
      epred=edge=>{
      edge.attr >= epsilon
    })

    val componentsGraph=cutOffGraph.connectedComponents()

    graph.outerJoinVertices(componentsGraph.vertices)((vId,oldData,newData)=>newData.get)
  }

}

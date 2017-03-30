package ml.sparkling.graph.operators.algorithms.community.pscan

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.{CommunityDetectionAlgorithm, ComponentID}
import ml.sparkling.graph.operators.measures.utils.CollectionsUtils._
import ml.sparkling.graph.operators.measures.utils.NeighboursUtils
import ml.sparkling.graph.operators.measures.utils.NeighboursUtils.NeighbourSet
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
case object PSCAN extends CommunityDetectionAlgorithm{
  val defaultComponentId: ComponentID = -1

  def computeConnectedComponents[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],epsilon:Double=0.05):Graph[ComponentID,ED]={

    val neighbours: Graph[NeighbourSet, ED] = NeighboursUtils.getWithNeighbours(graph,treatAsUndirected = true)

    val edgesWithSimilarity=neighbours.mapTriplets(edge=>{
      val sizeOfIntersection=intersectSize(edge.srcAttr,edge.dstAttr)
      val denominator = Math.sqrt(edge.srcAttr.size()*edge.dstAttr.size())
      sizeOfIntersection/denominator
    }).cache()

    val cutOffGraph=edgesWithSimilarity.filter[NeighbourSet, Double](
      preprocess=g=>g,
      epred=edge=>{
        edge.attr >= epsilon
      }).cache()

    val componentsGraph=cutOffGraph.connectedComponents()

    graph.outerJoinVertices(componentsGraph.vertices)((vId,oldData,newData)=>{
      newData.getOrElse(defaultComponentId)
    })
  }

  def computeConnectedComponentsUsing[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],requiredNumberOfComponents:Int=32):Graph[ComponentID,ED]={

    val neighbours: Graph[NeighbourSet, ED] = NeighboursUtils.getWithNeighbours(graph,treatAsUndirected = true)

    val edgesWithSimilarity=neighbours.mapTriplets(edge=>{
      val sizeOfIntersection=intersectSize(edge.srcAttr,edge.dstAttr)
      val denominator = Math.sqrt(edge.srcAttr.size()*edge.dstAttr.size())
      sizeOfIntersection/denominator
    }).cache()
    val edgesWeights=edgesWithSimilarity.edges.map(_.attr).distinct().sortBy(t=>t).collect();
    var min=0
    var max=edgesWeights.length-1
    var wholeMax=edgesWeights.length-1
    var components=edgesWithSimilarity.connectedComponents()
    var numberOfComponents=components.vertices.map(_._2).distinct().count()
    var found=false;
    while(!found){
      val index=Math.floor((min+max)/2.0).toInt
      val cutOffValue= edgesWeights(index);
      val cutOffGraph=edgesWithSimilarity.filter[NeighbourSet, Double](
        preprocess=g=>g,
        epred=edge=>{
          edge.attr >cutOffValue
        }).cache()
      val componentsGraph=cutOffGraph.connectedComponents()
      val currentNumberOfComponents=componentsGraph.vertices.map(_._2).distinct().count()
      if(Math.abs(requiredNumberOfComponents-currentNumberOfComponents)<Math.abs(requiredNumberOfComponents-numberOfComponents)){
        components=componentsGraph;
        numberOfComponents=currentNumberOfComponents;
      }
      if(min==max||index==0||index==wholeMax){
        found=true;
      }else{
        if(currentNumberOfComponents<requiredNumberOfComponents){
          min=index-1
        }else if(currentNumberOfComponents>requiredNumberOfComponents){
          max=index-1
        }else{
          found=true
        }
      }
    }


    graph.outerJoinVertices(components.vertices)((vId,oldData,newData)=>{
      newData.getOrElse(defaultComponentId)
    })
  }

  override def detectCommunities[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[ComponentID, ED] = {
    computeConnectedComponents(graph)
  }

}

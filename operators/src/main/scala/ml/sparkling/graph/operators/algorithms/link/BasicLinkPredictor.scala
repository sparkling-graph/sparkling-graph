package ml.sparkling.graph.operators.algorithms.link

import ml.sparkling.graph.api.operators.algorithms.link.MeasureBasedLnkPredictor
import ml.sparkling.graph.api.operators.measures.EdgeMeasure
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object BasicLinkPredictor extends MeasureBasedLnkPredictor {

  override def predictLinks[V: ClassTag, E: ClassTag, EV: ClassTag, EO: ClassTag](graph: Graph[V, E],
                                                                                  edgeMeasure: EdgeMeasure[EO, EV],
                                                                                  threshold: EO,
                                                                                  treatAsUndirected:Boolean=false)(implicit num: Numeric[EO]) = {
    val preprocessedGraph=edgeMeasure.preprocess(graph,treatAsUndirected)
    val allPossibleEdges = preprocessedGraph.vertices.cartesian(preprocessedGraph.vertices).filter{
      case ((vId1,data1),(vId2,data2))=>vId1!=vId2
    }
    val edgesAboveThreshold=allPossibleEdges.map{
      case ((vId1,data1),(vId2,data2))=>(edgeMeasure.computeValue(data1,data2,treatAsUndirected),(vId1,vId2))
    }.filter(t=>num.gt(t._1,threshold)).map(t=>(t._2,0))
    val exsistingEdgesTuples=graph.edges.map(e=>((e.srcId,e.dstId),0))
    val newEdges=edgesAboveThreshold.leftOuterJoin(exsistingEdgesTuples).filter{
      case (k,(_,option))=>option.isEmpty
    }.map(_._1)
    if(treatAsUndirected){
      newEdges.map{
        case (vId1,vId2)=>(Math.min(vId1,vId2),Math.max(vId1,vId2))
      }.distinct()
    }else{
      newEdges
    }
  }

}

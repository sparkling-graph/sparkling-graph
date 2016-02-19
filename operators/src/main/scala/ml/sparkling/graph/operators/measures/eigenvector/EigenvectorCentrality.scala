package ml.sparkling.graph.operators.measures.eigenvector

import ml.sparkling.graph.api.operators.measures.{VertexMeasureConfiguration, VertexMeasure}
import org.apache.spark.graphx._
import EigenvectorUtils._

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object EigenvectorCentrality extends VertexMeasure[Double]{

  /**
   * Generic Eigenvector Centrality computation method, should be used for extensions, computations are done until @continuePredicate gives true
   * @param graph - computation graph
   * @param vertexMeasureConfiguration - configuration of computation
   * @param continuePredicate - convergence predicate
   * @param num - numeric for @ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its eigenvector
   */
  def computeEigenvector[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],
                                                 vertexMeasureConfiguration: VertexMeasureConfiguration[VD,ED],
                      continuePredicate:ContinuePredicate=convergencePredicate(1e-6))(implicit num:Numeric[ED])={
    val numberOfNodes=graph.numVertices
    val startingValue=1.0/numberOfNodes
    var computationGraph=graph.mapVertices((vId,data)=>startingValue)
    var iteration=0
    var oldValue=0d
    var newValue=0d
    while(continuePredicate(iteration,oldValue,newValue)||iteration==0){
      val iterationRDD=computationGraph.aggregateMessages[Double](
      sendMsg = (context)=>{
        context.sendToDst(num.toDouble(context.attr)*context.srcAttr)
        context.sendToSrc(0.)
        if(vertexMeasureConfiguration.treatAsUndirected){
          context.sendToSrc(num.toDouble(context.attr)*context.dstAttr)
          context.sendToDst(0.)
        }
      },
      mergeMsg = (a,b)=>a+b)
      val normalizationValue=Math.sqrt(iterationRDD.map(e=>Math.pow(e._2,2)).sum())
      computationGraph=computationGraph.outerJoinVertices(iterationRDD)((vId,oldValue,newValue)=>if(normalizationValue==0) 0 else newValue.getOrElse(0d)/normalizationValue)
      oldValue=newValue
      newValue=computationGraph.vertices.map(e=>e._2).sum()/numberOfNodes
      iterationRDD.unpersist()
      iteration+=1
    }
    computationGraph
  }
  /**
   * Computes Eigenvector Centrality for each vertex in graph
   * @param graph - computation graph
   * @param vertexMeasureConfiguration - configuration of computation
   * @param num - numeric for @ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its eigenvector
   */
  override def compute[VD:ClassTag, ED:ClassTag](graph: Graph[VD, ED],vertexMeasureConfiguration: VertexMeasureConfiguration[VD,ED])(implicit num:Numeric[ED]): Graph[Double, ED] = computeEigenvector(graph,vertexMeasureConfiguration)
 }

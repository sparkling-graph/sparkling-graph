package ml.sparkling.graph.operators.measures.vertex.hits

import ml.sparkling.graph.api.operators.measures.{VertexMeasure, VertexMeasureConfiguration}
import ml.sparkling.graph.operators.measures.vertex.hits.HitsUtils._
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object Hits extends VertexMeasure[(Double, Double)] {
  /**
   * Generic hits (hub,auth) computation method, should be used for extensions, computations are done until @continuePredicate gives true
   * @param graph - computation graph
   * @param continuePredicate - convergence predicate
   * @param normalize - if true, output values are normalized
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its hits (hub,auth) values
   */
  def computeBasic[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], continuePredicate: ContinuePredicate = convergencePredicate(1e-8), normalize: Boolean = true) = {
    var iteration = 0
    var oldValues = (0d, 0d) // (hub,auth)
    var newValues = (0d, 0d)
    val numVertices = graph.numVertices
    var computationGraph = graph.mapVertices((vId, data) => (1d / numVertices, 1d / numVertices))
    while (continuePredicate(iteration, oldValues, newValues) || iteration == 0) {
      val withNewAuths = computationGraph.aggregateMessages[Double](
        sendMsg = context=>{
          val sourceHub: Double = context.srcAttr match{
           case  (hub,auth) => hub
          }
          context.sendToDst(sourceHub)
          context.sendToSrc(0d)
          },
        mergeMsg = (a,b)=>a+b)
      val normAuths = withNewAuths.map{case (vId,auth) => auth}.max()
      computationGraph = computationGraph.outerJoinVertices(withNewAuths){
        case (vId, (hub,auth), Some(newValue)) => (hub, newValue / normAuths)
        case (vId, (hub,auth), None) => (hub, 0d)
      }
      withNewAuths.unpersist()
      val withNewHubs = computationGraph.aggregateMessages[Double](
        sendMsg = context=>{
          val destinationAuth: Double = context.dstAttr match{
           case  (hub,auth) => auth
          }
          context.sendToSrc(destinationAuth)
          context.sendToDst(0d)
          },
        mergeMsg = (a,b)=>a+b)
      val normHubs = withNewHubs.map{case (vId,hub) => hub}.max()
      computationGraph = computationGraph.outerJoinVertices(withNewHubs){
        case (vId, (hub,auth), Some(newValue)) => (newValue/normHubs, auth)
        case (vId, (hub,auth), None) => (0d, auth)
      }
      withNewHubs.unpersist()
      oldValues = newValues
      newValues = computationGraph.vertices.map{case (vId,(hub,auth)) => (hub,auth)}.fold((0d, 0d))(sumHubAuthTuples)
      newValues = newValues match{
        case (hub,auth)=> (hub/numVertices,auth/numVertices)
      }
      iteration += 1
    }
    if (normalize) {
      val sum = computationGraph.vertices.values.fold((0d, 0d))(sumHubAuthTuples)
      computationGraph.mapVertices(normalizeHubAuthBy(sum))
    } else {
      computationGraph
    }
  }

  def normalizeHubAuthBy(denominator:(Double,Double))(vId:VertexId,data:(Double,Double)):(Double,Double)={
    (denominator,data) match{
      case ((hubDenominator,authDenominator),(hub,auth))=>
        val normalizedHub=hub/hubDenominator
        val normalizedAuth=auth/authDenominator
        (normalizedHub,normalizedAuth)
    }
  }
  
  def sumHubAuthTuples(t1:(Double,Double),t2:(Double,Double)):(Double,Double)={
    (t1,t2) match{
      case ((hub1,auth1),(hub2,auth2))=>
        val sumHub=hub1+hub2
        val sumAuth=auth1+auth2
        (sumHub,sumAuth)
    }
  }

  /**
   * Computes normalized hits (hub,auth) for each vertex
   * @param graph - computation graph
   * @param vertexMeasureConfiguration - configuration of computation
   * @param num - numeric for @ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its hits values (hub,auth)
   */
  override def compute[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED])(implicit num: Numeric[ED]): Graph[(Double, Double), ED] = computeBasic(graph)
}

package ml.sparkling.graph.operators.measures.hits

import ml.sparkling.graph.api.operators.measures.{VertexMeasureConfiguration, VertexMeasure}
import org.apache.spark.graphx.Graph
import HitsUtils._

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object Hits extends VertexMeasure[(Double,Double)]{
  /**
   * Generic hits (hub,auth) computation method, should be used for extensions, computations are done until @continuePredicate gives true
   * @param graph - computation graph
   * @param continuePredicate - convergence predicate
   * @param normalize - if true, output values are normalized
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its eigenvector
   */
  def computeHits[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], continuePredicate: ContinuePredicate = convergencePredicate(1e-8), normalize: Boolean = true) = {
    var iteration = 0
    var oldValues = (0d, 0d) // (hub,auth)
    var newValues = (0d, 0d)
    val numVertices = graph.numVertices
    var computationGraph = graph.mapVertices((vId, data) => (1d / numVertices, 1d / numVertices))
    while (continuePredicate(iteration, oldValues, newValues) || iteration == 0) {
      val newAuths = computationGraph.aggregateMessages[Double](
        sendMsg = (context)=>{
          context.sendToDst(context.srcAttr._1)
          context.sendToSrc(0d)
          },
        mergeMsg = (a,b)=>a+b)
      val normAuths = newAuths.map(e => e._2).max()
      computationGraph = computationGraph.outerJoinVertices(newAuths)((vId, oldValue, newValue) => (oldValue._1, newValue.getOrElse(0d) / normAuths))
      newAuths.unpersist()
      val newHubs = computationGraph.aggregateMessages[Double](
        sendMsg = (context)=>{
          context.sendToSrc(context.dstAttr._2)
          context.sendToDst(0d)
          },
        mergeMsg = (a,b)=>a+b)
      val normHubs = newHubs.map(e => e._2).max()
      computationGraph = computationGraph.outerJoinVertices(newHubs)((vId, oldValue, newValue) => (newValue.getOrElse(0d) / normHubs, oldValue._2))
      newHubs.unpersist()
      oldValues = newValues
      newValues = computationGraph.vertices.map(t => t._2).fold((0d, 0d))((a, b) => (a._1 + b._1, a._2 + b._2))
      newValues = (newValues._1 / numVertices, newValues._2 / numVertices)
      iteration += 1
    }
    if (normalize) {
      val sum = computationGraph.vertices.map(t => t._2).fold((0d, 0d))((a, b) => (a._1 + b._1, a._2 + b._2))
      computationGraph.mapVertices((vId, data) => (data._1 / sum._1, data._2 / sum._2))
    } else {
      computationGraph
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
  override def compute[VD:ClassTag, ED:ClassTag](graph: Graph[VD, ED],vertexMeasureConfiguration: VertexMeasureConfiguration[VD,ED])(implicit num:Numeric[ED]): Graph[(Double, Double), ED] = computeHits(graph)
}

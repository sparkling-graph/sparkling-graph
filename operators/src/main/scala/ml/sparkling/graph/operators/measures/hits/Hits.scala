package ml.sparkling.graph.operators.measures.hits

import ml.sparkling.graph.api.operators.measures.{VertexMeasure, VertexMeasureConfiguration}
import ml.sparkling.graph.operators.measures.hits.HitsUtils._
import org.apache.spark.graphx.{VertexId, Graph}
import org.apache.spark.rdd.RDD

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
  def computeHits[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], continuePredicate: ContinuePredicate = convergencePredicate(1e-8), normalize: Boolean = true) = {
    var iteration = 0
    var oldValues = (0d, 0d) // (hub,auth)
    var newValues = (0d, 0d)
    val numVertices = graph.numVertices
    var computationGraph = graph.mapVertices((vId, data) => (1d / numVertices, 1d / numVertices))
    while (continuePredicate(iteration, oldValues, newValues) || iteration == 0) {
      val withNewAuths = computationGraph.aggregateMessages[Double](
        sendMsg = (context)=>{
          val sourceHub: Double = context.srcAttr._1
          context.sendToDst(sourceHub)
          context.sendToSrc(0d)
          },
        mergeMsg = (a,b)=>a+b)
      val newAuths: RDD[Double] = withNewAuths.map(e => e._2)
      val normAuths = newAuths.max()
      computationGraph = computationGraph.outerJoinVertices(withNewAuths)((vId, oldValue, newValue) => (oldValue._1, newValue.getOrElse(0d) / normAuths))
      withNewAuths.unpersist()
      val withNewHubs = computationGraph.aggregateMessages[Double](
        sendMsg = (context)=>{
          val destinationAuth: Double = context.dstAttr._2
          context.sendToSrc(destinationAuth)
          context.sendToDst(0d)
          },
        mergeMsg = (a,b)=>a+b)
      val newHubs: RDD[Double] = withNewHubs.map(e => e._2)
      val normHubs = newHubs.max()
      computationGraph = computationGraph.outerJoinVertices(withNewHubs)((vId, oldValue, newValue) => (newValue.getOrElse(0d) / normHubs, oldValue._2))
      withNewHubs.unpersist()
      oldValues = newValues
      newValues = computationGraph.vertices.map(t => t._2).fold((0d, 0d))(sumHubAuthTuples)
      newValues = (newValues._1 / numVertices, newValues._2 / numVertices)
      iteration += 1
    }
    if (normalize) {
      val sum = computationGraph.vertices.map(t => t._2).fold((0d, 0d))(sumHubAuthTuples)
      computationGraph.mapVertices(normalizeHubAuthBy(sum))
    } else {
      computationGraph
    }
  }

  def normalizeHubAuthBy(denominator:(Double,Double))(vId:VertexId,data:(Double,Double)):(Double,Double)={
    val normalizedHub=data._1/denominator._1
    val normalizedAuth=data._2/denominator._2
    (normalizedHub,normalizedAuth)
  }
  
  def sumHubAuthTuples(t1:(Double,Double),t2:(Double,Double)):(Double,Double)={
    val sumHub=t1._1+t2._1
    val sumAuth=t1._2+t2._2
    (sumHub,sumAuth)
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
  override def compute[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED])(implicit num: Numeric[ED]): Graph[(Double, Double), ED] = computeHits(graph)
}

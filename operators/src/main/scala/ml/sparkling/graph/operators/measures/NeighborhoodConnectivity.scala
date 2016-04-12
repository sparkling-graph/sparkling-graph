package ml.sparkling.graph.operators.measures

import ml.sparkling.graph.api.operators.measures.{VertexMeasure, VertexMeasureConfiguration}
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object NeighborhoodConnectivity extends VertexMeasure[Double] {

  /**
   * Generic Neighborhood Connectivity method, should be used for extensions
   * @param graph - computation graph
   * @param vertexMeasureConfiguration - configuration of computation
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its  neighbour connectivity
   */
  def computeNeighborConnectivity[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED]) = {
    val graphWithInOutDegree=Degree.computeInOut(graph, vertexMeasureConfiguration)
    val graphWithOutDegree =graphWithInOutDegree.mapVertices{
      case (vId, (out,in)) => out
    }
    val connectivityRdd = graphWithOutDegree.mapVertices((vId,degree)=>(degree,1))
      .aggregateMessages[(Int,Int)](
        sendMsg=context=>{
          context.sendToSrc(context.dstAttr)
          context.sendToDst((0,0))
          if(vertexMeasureConfiguration.treatAsUndirected) {
            context.sendToDst(context.srcAttr)
            context.sendToSrc((0,0))
          }
        },
        mergeMsg=addTuples)
      .mapValues(
        t => t match {
          case (degresSum, countsSum) if countsSum !=0 => degresSum.toDouble/countsSum
          case   _ => 0d
        }
    )
    graph.outerJoinVertices(connectivityRdd)((vId, oldValue, newValue) => newValue.getOrElse(0d))
  }

  def addTuples(t1:(Int,Int),t2:(Int,Int)):(Int,Int)={
    (t1,t2) match{
      case ((t1Degress,t1Counts),(t2Degress,t2Counts))=>
        val sumDegress=t1Degress+t2Degress
        val sumCounts=t1Counts+t2Counts
        (sumDegress,sumCounts)
    }

  }

  /**
   * Computes Neighborhood Connectivity  of each vertex
   * @param graph - computation graph
   * @param vertexMeasureConfiguration - configuration of computation
   * @param num - numeric for @ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its  neighbour connectivity
   */
  override def compute[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED])(implicit num: Numeric[ED]) = {
    computeNeighborConnectivity(graph, vertexMeasureConfiguration)
  }
}

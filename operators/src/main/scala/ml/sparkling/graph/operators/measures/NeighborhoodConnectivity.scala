package ml.sparkling.graph.operators.measures

import ml.sparkling.graph.api.operators.measures.{VertexMeasureConfiguration, VertexMeasure}
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
    val graphWithInOutDegree=Degree.computeDegree(graph, vertexMeasureConfiguration)
    val graphWithOutDegree =graphWithInOutDegree.mapVertices((vId, data) => data._1)
    val connectivityRdd = graphWithOutDegree.mapVertices((vId,degree)=>(degree,1))
      .aggregateMessages[(Int,Int)](
        sendMsg=(context)=>{
          context.sendToSrc(context.dstAttr)
          context.sendToDst((0,0))
          if(vertexMeasureConfiguration.treatAsUndirected) {
            context.sendToDst(context.srcAttr)
            context.sendToSrc((0,0))
          }
        },
        mergeMsg= (t1,t2) => (t1._1+t2._1,t1._2+t2._2))
      .mapValues(t => if (t._2 == 0) 0 else t._1.toDouble / t._2)
    graph.outerJoinVertices(connectivityRdd)((vId, oldValue, newValue) => newValue.getOrElse(0d))
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

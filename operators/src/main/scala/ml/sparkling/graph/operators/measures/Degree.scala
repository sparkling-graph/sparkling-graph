package ml.sparkling.graph.operators.measures

import ml.sparkling.graph.api.operators.measures.{VertexMeasure, VertexMeasureConfiguration}
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Computes degree of each vertex
 */
object Degree extends VertexMeasure[(Int, Int)] {
  /**
   * Generic degree method, should be used for extensions, returns degree in format (inDegree,outDegree)
   * @param graph - computation graph
   * @param vertexMeasureConfiguration - configuration of computation
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its  degree (out,in)
   */
  def computeInOut[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED]) = {
    vertexMeasureConfiguration.treatAsUndirected match{
      case true  =>     graph.outerJoinVertices[Int,(Int,Int)](graph.degrees)((vId,oldValue,newValue)=>(newValue.getOrElse(0),newValue.getOrElse(0)))
      case _ =>   graph.outerJoinVertices[Int,Int](graph.outDegrees)((vId,oldValue,newValue)=>newValue.getOrElse(0))
      .outerJoinVertices[Int,(Int,Int)](graph.inDegrees)((vId,oldValue,newValue)=>(oldValue,newValue.getOrElse(0)))
    }
  }

  /**
   * Computes degree of each vertex, returns degree in format (inDegree,outDegree)
   * @param graph - computation graph
   * @param vertexMeasureConfiguration - configuration of computation
   * @param num - numeric for @ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its  degree (out,in)
   */
  override def compute[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED])(implicit num: Numeric[ED]) = {
    computeInOut(graph, vertexMeasureConfiguration)
  }

}

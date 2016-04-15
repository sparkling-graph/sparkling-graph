package ml.sparkling.graph.api.operators.algorithms.link

import ml.sparkling.graph.api.operators.measures.EdgeMeasure
import org.apache.spark.graphx.{VertexId, Graph}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Trait for basic link prediction. Implementation should use similarity measure of type EdgeMeasure
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
trait MeasureBasedLnkPredictor {

  /**
   * Implementation should predict new links in given graph using suplied vertex similarity measure.
   * @param graph - input graph
   * @param edgeMeasure - similarity measure
   * @param threshold - minimal similarity measure value
   * @param treatAsUndirected - true if graph should be treated as undirected
   * @param num - numeric of similarity measure
   * @tparam V
   * @tparam E
   * @tparam EV
   * @tparam EO
   * @return
   */
  def predictLinks[V:ClassTag,E:ClassTag,EV:ClassTag,EO:ClassTag](graph:Graph[V,E],
                                                                  edgeMeasure: EdgeMeasure[EO,EV],
                                                                  threshold:EO,
                                                                   treatAsUndirected:Boolean)
                                                                 (implicit num:Numeric[EO]):RDD[(VertexId,VertexId)]

}

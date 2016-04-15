package ml.sparkling.graph.api.operators.algorithms.link

import ml.sparkling.graph.api.operators.measures.EdgeMeasure
import org.apache.spark.graphx.{VertexId, Graph}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
trait MeasureBasedLnkPredictor {

  def predictLinks[V:ClassTag,E:ClassTag,EV:ClassTag,EO:ClassTag](graph:Graph[V,E],
                                                                  edgeMeasure: EdgeMeasure[EO,EV],
                                                                  threshold:EO,
                                                                   treatAsUndirected:Boolean)
                                                                 (implicit num:Numeric[EO]):RDD[(VertexId,VertexId)]

}

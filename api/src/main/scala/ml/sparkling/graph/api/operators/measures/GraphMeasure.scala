package ml.sparkling.graph.api.operators.measures

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
trait GraphMeasure[OV] {
  def compute[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED])(implicit num:Numeric[ED]):OV
 }

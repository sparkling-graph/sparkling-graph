package ml.sparkling.graph.api.operators.measures

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
trait GraphIndependentMeasure[OV] {
  def compute[V:ClassTag,E:ClassTag](graph:Graph[V,E]):OV
 }

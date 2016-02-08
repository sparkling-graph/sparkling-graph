package ml.sparkling.graph.operators

import ml.sparkling.graph.api.operators.measures.{VertexMeasureConfiguration, VertexMeasure}
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Standard GraphX PageRank wrapper
 */
object PageRank extends VertexMeasure[Double] {
  override def compute[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],vertexMeasureConfiguration: VertexMeasureConfiguration[VD,ED])(implicit num: Numeric[ED]) = {
    org.apache.spark.graphx.lib.PageRank.run(graph,1000)
  }
}

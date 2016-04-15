package ml.sparkling.graph.operators.measures.graph

import ml.sparkling.graph.api.operators.measures.{GraphIndependentMeasure}
import org.apache.spark.graphx.{Graph, VertexRDD}

import scala.reflect.ClassTag


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object FreemanCentrality extends GraphIndependentMeasure[Double]{

  def compute[K:ClassTag,E:ClassTag](graph: Graph[K,E]): Double = {
    val degrees: VertexRDD[Int] = graph.degrees

    val maxDegree=degrees.values.max()

    val numerator=degrees.values.map(maxDegree - _).sum()
    val denominator=(graph.numVertices-1)*(graph.numVertices-2)

    numerator/denominator.toDouble
  }

}

package ml.sparkling.graph.operators.measures

import ml.sparkling.graph.api.operators.measures.{GraphMeasure, VertexMeasure, VertexMeasureConfiguration}
import org.apache.spark.graphx.{VertexRDD, Graph}

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object FreemanCentrality extends GraphMeasure[Double] {

  override def compute[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED])(implicit num: Numeric[ED]): Double = {
    val degrees: VertexRDD[Int] = graph.degrees

    val maxDegree=degrees.values.max()

    val numerator=degrees.values.map(maxDegree - _).sum()
    val denominator=(graph.numVertices-1)*(graph.numVertices-2)

    numerator/denominator.toDouble
  }
  
}

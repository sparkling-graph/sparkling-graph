package ml.sparkling.graph.operators

import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class DSLTest (implicit sc:SparkContext)  extends MeasureTest {

  val graph:Graph[Int,Int]= ???
  
  private val centrality: Graph[Double, _] = graph.closenessCentrality(VertexMeasureConfiguration((g:Graph[_,_])=>10l))
  
}

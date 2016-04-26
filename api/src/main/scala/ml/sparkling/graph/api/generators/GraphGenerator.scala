package ml.sparkling.graph.api.generators

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 26.04.16.
  */
abstract class GraphGenerator[PT<:GraphGeneratorConfiguration,ET,VT] {
def generate(configuration:PT)(implicit ctx:SparkContext):Graph[VT,ET]
}

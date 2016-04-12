package ml.sparkling.graph.loaders.csv.types

import org.apache.spark.graphx.VertexId

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object Types {
  type ToVertexId[VT]=VT => VertexId
}

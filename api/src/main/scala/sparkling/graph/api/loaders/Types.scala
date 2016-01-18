package sparkling.graph.api.loaders

import org.apache.spark.graphx._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object Types {
  type ToVertexId[VT]=VT => VertexId
}

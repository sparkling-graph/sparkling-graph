package sparkling.graph.loaders.csv.utils

import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.Row

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object DefaultTransformers {

  def defaultEdgeAttribute(row: Row):Double = {
    1d
  }

  def numberToVertexId[VD](any: VD):VertexId = {
    any.toString.toLong
  }

}

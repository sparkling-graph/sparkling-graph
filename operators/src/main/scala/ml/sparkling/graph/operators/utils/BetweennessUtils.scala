package ml.sparkling.graph.operators.utils

import org.apache.spark.graphx.VertexRDD

/**
  * Created by mth on 6/26/17.
  */
object BetweennessUtils extends Serializable {

  def normalize(vertices: VertexRDD[Double], directed: Boolean) = {
    val n = vertices.count()
    val denominator = if (directed) (n - 1) * (n - 2) else (n - 1) * (n - 2) / 2
    vertices.mapValues(_ / denominator)
  }
}

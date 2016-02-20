package ml.sparkling.graph.loaders.csv.providers

import ml.sparkling.graph.loaders.csv.types.Types
import ml.sparkling.graph.loaders.csv.utils.DefaultTransformers
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.Row
import Types.ToVertexId

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object VertexProviders {

  def columnsAsVertex[VT](ids: Seq[Int], row: Row,columnToId: ToVertexId[VT] = DefaultTransformers.numberToVertexId _
                        ): Seq[(VertexId, VT)] = {
    ids.map(row.getAs[VT](_)).map(value => (columnToId(value), value))
  }

}

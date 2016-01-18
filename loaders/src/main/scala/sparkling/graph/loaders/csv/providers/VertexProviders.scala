package sparkling.graph.loaders.csv.providers

import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.Row
import sparkling.graph.api.loaders.Types.ToVertexId
import sparkling.graph.loaders.csv.utils.DefaultTransformers

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object VertexProviders {

  def columnsAsVertex[VT](ids: Seq[Int], row: Row,columnToId: ToVertexId[VT] = DefaultTransformers.numberToVertexId _
                        ): Seq[(VertexId, VT)] = {
    ids.map(row.getAs[VT](_)).map(value => (columnToId(value), value))
  }

}

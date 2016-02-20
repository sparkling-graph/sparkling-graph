package ml.sparkling.graph.loaders.csv.types

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object CSVTypes {
  type GraphBuilder[VD,ED]=(DataFrame) => Graph[VD, ED]
  type EdgeAttributeExtractor[ED]=Row=>ED
}

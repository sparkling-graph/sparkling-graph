package ml.sparkling.graph.loaders.csv.providers

import ml.sparkling.graph.api.loaders.{CSVTypes, Types}
import ml.sparkling.graph.loaders.csv.utils.DefaultTransformers
import org.apache.spark.graphx._
import org.apache.spark.sql.Row
import CSVTypes.EdgeAttributeExtractor
import Types.ToVertexId
import DefaultTransformers.{defaultEdgeAttribute, numberToVertexId}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object EdgeProviders {

  def twoColumnsMakesEdge[VD,ED](id1:Int,
                          id2:Int,row:Row,
                          columnToId:ToVertexId[VD],
                          edgeAttributeProvider:EdgeAttributeExtractor[ED]):Seq[Edge[ED]]={
   Seq(Edge(columnToId(row.getAs(id1)),columnToId(row.getAs(id2)),edgeAttributeProvider(row)))
  }

  def twoColumnsMakesEdge[VD](id1:Int,
                                 id2:Int,
                                 row:Row):Seq[Edge[Double]]={
    twoColumnsMakesEdge(id1,id2,row,numberToVertexId _,defaultEdgeAttribute _)
  }

}

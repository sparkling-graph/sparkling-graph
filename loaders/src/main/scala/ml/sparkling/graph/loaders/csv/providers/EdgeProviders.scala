package ml.sparkling.graph.loaders.csv.providers

import ml.sparkling.graph.loaders.csv.types.{CSVTypes, Types}
import ml.sparkling.graph.loaders.csv.utils.DefaultTransformers
import org.apache.spark.graphx._
import org.apache.spark.sql.Row
import CSVTypes.EdgeAttributeExtractor
import Types.ToVertexId
import DefaultTransformers.{defaultEdgeAttribute, numberToVertexId}

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object EdgeProviders {

  type TwoColumnsMakeEdgeProvider[VD,ED]=(Int,Int,Row, ToVertexId[VD], EdgeAttributeExtractor[ED])=>Seq[Edge[ED]]

  def twoColumnsMakesEdge[VD:ClassTag,ED:ClassTag](id1:Int,
                          id2:Int,row:Row,
                          columnToId:ToVertexId[VD],
                          edgeAttributeProvider:EdgeAttributeExtractor[ED]):Seq[Edge[ED]]={
   Seq(Edge(columnToId(row.getAs(id1)),columnToId(row.getAs(id2)),edgeAttributeProvider(row)))
  }

  def twoColumnsMakesEdge[VD:ClassTag](id1:Int,
                                 id2:Int,
                                 row:Row):Seq[Edge[Double]]={
    twoColumnsMakesEdge(id1,id2,row,numberToVertexId _,defaultEdgeAttribute _)
  }

}

package ml.sparkling.graph.operators.measures.edge

import ml.sparkling.graph.api.operators.measures.EdgeMeasure
import ml.sparkling.graph.operators.measures.utils.NeighboursUtils
import ml.sparkling.graph.operators.measures.utils.NeighboursUtils.NeighbourSet
import org.apache.spark.graphx.{Graph, EdgeTriplet}
import ml.sparkling.graph.operators.measures.utils.CollectionsUtils._

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object CommonNeighbours extends EdgeMeasure[Int,NeighbourSet]{

  def computeValue[E:ClassTag](srcAttr:NeighbourSet,dstAttr:NeighbourSet,treatAsUndirected:Boolean=false):Int={
    intersectSize(srcAttr,dstAttr)
  }

  override def preprocess[VD:ClassTag,E:ClassTag](graph: Graph[VD, E],treatAsUndirected:Boolean=false): Graph[NeighbourSet, E] = {
    NeighboursUtils.getWithNeighbours(graph,treatAsUndirected)
  }
}

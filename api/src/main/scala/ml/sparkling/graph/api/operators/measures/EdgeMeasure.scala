package ml.sparkling.graph.api.operators.measures

import org.apache.spark.graphx.{Graph, EdgeTriplet}

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
trait EdgeMeasure[O,V] extends Serializable{
  def computeValue(srcAttr:V,dstAttr:V,treatAsUndirected:Boolean=false):O

  def compute[E:ClassTag](graph:Graph[V,E],treatAsUndirected:Boolean=false)(implicit ot:ClassTag[O]): Graph[V,O] ={
    graph.mapTriplets(e=>computeValue(e.srcAttr,e.dstAttr,treatAsUndirected))
  }

  def preprocess[VD:ClassTag,E:ClassTag](graph:Graph[VD,E],treatAsUndirected:Boolean=false):Graph[V,E]

  def computeWithPreprocessing[VD:ClassTag,E:ClassTag](graph:Graph[VD,E],treatAsUndirected:Boolean=false)(implicit ot:ClassTag[O]): Graph[V,O] ={
    compute(preprocess(graph,treatAsUndirected),treatAsUndirected)
  }
}

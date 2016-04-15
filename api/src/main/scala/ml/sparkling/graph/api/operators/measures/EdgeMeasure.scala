package ml.sparkling.graph.api.operators.measures

import org.apache.spark.graphx.{Graph, EdgeTriplet}

import scala.reflect.ClassTag

/**
 * Similarity measure of two vertices (or measure for given edge)
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
trait EdgeMeasure[O,V] extends Serializable{
  /**
   * Computes measure for pair of vertices attriubtes
   * @param srcAttr
   * @param dstAttr
   * @param treatAsUndirected
   * @return
   */
  def computeValue(srcAttr:V,dstAttr:V,treatAsUndirected:Boolean=false):O

  /**
   * Computes measure for give graph that has values expected by measure
   * @param graph
   * @param treatAsUndirected
   * @param ot
   * @tparam E
   * @return
   */
  def compute[E:ClassTag](graph:Graph[V,E],treatAsUndirected:Boolean=false)(implicit ot:ClassTag[O]): Graph[V,O] ={
    graph.mapTriplets(e=>computeValue(e.srcAttr,e.dstAttr,treatAsUndirected))
  }

  /**
   * Prepares graph for measure computation
   * @param graph
   * @param treatAsUndirected
   * @tparam VD
   * @tparam E
   * @return
   */
  def preprocess[VD:ClassTag,E:ClassTag](graph:Graph[VD,E],treatAsUndirected:Boolean=false):Graph[V,E]

  /**
   * Prepares graph for measure computation and do computation
   * @param graph
   * @param treatAsUndirected
   * @param ot
   * @tparam VD
   * @tparam E
   * @return
   */
  def computeWithPreprocessing[VD:ClassTag,E:ClassTag](graph:Graph[VD,E],treatAsUndirected:Boolean=false)(implicit ot:ClassTag[O]): Graph[V,O] ={
    compute(preprocess(graph,treatAsUndirected),treatAsUndirected)
  }
}

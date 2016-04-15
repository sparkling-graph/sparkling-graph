package ml.sparkling.graph.operators.measures.graph

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection.ComponentID
import ml.sparkling.graph.api.operators.measures.{VertexDependentGraphMeasure, GraphIndependentMeasure}
import org.apache.spark.graphx.{EdgeTriplet, VertexRDD, Graph}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object Modularity extends VertexDependentGraphMeasure[Double,ComponentID]{

   def compute[V<:ComponentID:ClassTag,E:ClassTag](graph: Graph[V, E]): Double = {
     val edgesNum=graph.numEdges.toDouble;
     val edgesCounts: RDD[(V, (Int, Int))] = graph.triplets.flatMap(triplet => {
       if (triplet.srcAttr == triplet.dstAttr) {
         Iterator((triplet.srcAttr, (1, 0)))
       } else {
         Iterator((triplet.srcAttr, (0, 1)))
       }
     })
     edgesCounts.groupByKey().map[Double]{
      case (communityId,data)=>
        val reduced=data.reduce[(Int,Int)]{
        case ((e1,a1),(e2,a2))=>(e1+e2,a1+a2)
      }
        (reduced._1/edgesNum)-Math.pow(reduced._2/edgesNum,2)
    }.sum()

  }

}

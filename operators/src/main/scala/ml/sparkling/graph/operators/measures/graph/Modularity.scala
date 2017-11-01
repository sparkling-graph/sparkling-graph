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
         Iterator((triplet.srcAttr, (1, 0)),(triplet.srcAttr, (1, 0)))
       } else {
         Iterator((triplet.srcAttr, (0, 1)),(triplet.dstAttr,(0,1)))
       }
     })
     edgesCounts.aggregateByKey((0,0))(
       (agg:(Int,Int),data:(Int,Int))=>
         (agg,data) match{
           case ((a1,b1),(a2,b2))=>(a1+a2,b1+b2)
         },
     (agg1:(Int,Int),agg2:(Int,Int))=>{
       (agg1,agg2) match{
         case ((a1,b1),(a2,b2))=>(a1+a2,b1+b2)
       }
     }
     ).treeAggregate(0.0)(
       (agg:Double,data:(V,(Int,Int)))=>{
         data match{
           case (_,(edgesFull,edgesSome))=>
             agg+(edgesFull/(2.0*edgesNum))-Math.pow((edgesSome+edgesFull)/(2.0*edgesNum),2)
         }
       },
       (agg1,agg2)=>agg1+agg2
     )

  }

}

package ml.sparkling.graph.api.operators.algorithms.coarsening

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 06.02.17.
  */
object CoarseningAlgorithm {

  type Component=List[VertexId]

  trait EdgeValueSelector extends Serializable{
    def getValue[ED](e1:ED,e2:ED):ED=e1
  }

  object DefaultEdgeValueSelector extends EdgeValueSelector

  trait CoarseningAlgorithm {
    def coarse[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],treatAsUndirected:Boolean=false,checkpointingFrequency:Int=10,edgeValueSelector:EdgeValueSelector=DefaultEdgeValueSelector):Graph[Component,ED]
  }

  type CoarseningMethod[VD,ED]=(Graph[VD,ED])=>Graph[Component,ED]
}

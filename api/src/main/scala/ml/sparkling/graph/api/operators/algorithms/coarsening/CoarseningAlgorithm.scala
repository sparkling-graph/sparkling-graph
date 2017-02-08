package ml.sparkling.graph.api.operators.algorithms.coarsening

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 06.02.17.
  */
object CoarseningAlgorithm {

  type Component=List[VertexId]

  trait CoarseningAlgorithm {
    def coarse[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED]):Graph[Component,ED]
  }

  type CoarseningMethod[VD,ED]=(Graph[VD,ED])=>Graph[Component,ED]
}

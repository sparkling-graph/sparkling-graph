package ml.sparkling.graph.api.operators.algorithms.community

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object CommunityDetection {

  type ComponentID=Long

  trait CommunityDetectionAlgorithm {
    def detectCommunities[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED]):Graph[ComponentID,ED]
  }

  type CommunityDetectionMethod[VD,ED]=(Graph[VD,ED])=>Graph[ComponentID,ED]
}

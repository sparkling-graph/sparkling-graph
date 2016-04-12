package ml.sparkling.graph.api.operators.partitioning

import org.apache.spark.graphx.{EdgeTriplet, PartitionID}

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
abstract class DataAwarePartitionStrategy[VD:ClassTag,ED:ClassTag] {
  /** Returns the partition number for a given edge. */
  def getPartition(edgeTriplet:EdgeTriplet[VD,ED]): PartitionID
}

package ml.sparkling.graph.api.operators.measures

import ml.sparkling.graph.api.operators.IterativeComputation
import ml.sparkling.graph.api.operators.IterativeComputation._

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
case class VertexMeasureConfiguration[VD: ClassTag, ED: ClassTag](val bucketSizeProvider: BucketSizeProvider[VD, ED],
                                                                   val treatAsUndirected: Boolean = false)

object VertexMeasureConfiguration {

  def apply[VD:ClassTag, ED:ClassTag]() =
    new VertexMeasureConfiguration[VD, ED]( wholeGraphBucket[VD, ED] _)

  def apply[VD:ClassTag, ED:ClassTag](treatAsUndirected:Boolean) =
    new VertexMeasureConfiguration[VD, ED]( wholeGraphBucket[VD, ED] _,treatAsUndirected)

  def apply[VD:ClassTag, ED:ClassTag](treatAsUndirected:Boolean,bucketSizeProvider:BucketSizeProvider[VD, ED]) =
    new VertexMeasureConfiguration[VD, ED](bucketSizeProvider,treatAsUndirected)

  def apply[VD:ClassTag, ED:ClassTag](bucketSizeProvider:BucketSizeProvider[VD, ED]) =
    new VertexMeasureConfiguration[VD, ED](bucketSizeProvider)

}

package ml.sparkling.graph.operators.measures.utils

import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes
import ShortestPathsTypes._
import scala.collection.JavaConversions._
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object CollectionsUtils {
   def intersectSize(neighbours1:JSet[JLong],neighbours2:JSet[JLong])={
    neighbours1.intersect(neighbours2).size
  }
}

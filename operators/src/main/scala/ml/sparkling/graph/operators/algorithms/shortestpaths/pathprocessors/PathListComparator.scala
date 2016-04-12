package ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors

import java.util.Comparator

import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes.{JDouble, JList}
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object PathListComparator extends Comparator[JList[JDouble]] with Serializable{
override def compare(t: JList[JDouble], t1: JList[JDouble]): Int = java.lang.Double.compare(t.get(0),t1.get(0))
}
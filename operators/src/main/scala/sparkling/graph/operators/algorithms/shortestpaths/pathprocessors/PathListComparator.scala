package sparkling.graph.operators.algorithms.shortestpaths.pathprocessors


import sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes.{JDouble, JList}

import java.util.Comparator
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object PathListComparator extends Comparator[JList[JDouble]] with Serializable{
override def compare(t: JList[JDouble], t1: JList[JDouble]): Int = return java.lang.Double.compare(t.get(0),t1.get(0))
}
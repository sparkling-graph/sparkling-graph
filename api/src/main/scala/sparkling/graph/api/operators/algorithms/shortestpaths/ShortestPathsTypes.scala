package sparkling.graph.api.operators.algorithms.shortestpaths


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object ShortestPathsTypes {
  type JMap[K,V]=java.util.Map[K,V]
  type JLong=java.lang.Long
  type JDouble=java.lang.Double
  type JSet[T]=java.util.Set[T]
  type JList[T]=java.util.List[T]
  type JCollection[T]=java.util.Collection[T]
  type JPath=JList[JDouble]
  type JPathCollection=JSet[JPath]
  type WithPathContainer=JMap[JLong,JPathCollection]
}

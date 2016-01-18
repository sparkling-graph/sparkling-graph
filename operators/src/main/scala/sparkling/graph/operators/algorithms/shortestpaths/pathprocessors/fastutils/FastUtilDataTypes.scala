package sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils

import sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes.{JLong, JMap, JDouble, JList}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object FastUtilDataTypes {
  type FPath=JList[JDouble]
  type FPathCollection=JList[FPath]
  type FPathContainer=JMap[JLong,FPathCollection]
}

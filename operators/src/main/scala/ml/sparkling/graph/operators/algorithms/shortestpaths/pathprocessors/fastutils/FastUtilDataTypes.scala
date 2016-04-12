package ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils

import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes.{JDouble, JList, JLong, JMap}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object FastUtilDataTypes {
  type FPath=JList[JDouble]
  type FPathCollection=JList[FPath]
  type FPathContainer=JMap[JLong,FPathCollection]
}

package ml.sparkling.graph.operators.measures.vertex.closenes

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object ClosenessUtils {

  type ClosenessFunction=(Long,Double,Boolean)=>Double
  type PathMappingFunction=(Double)=>Double

  def standardCloseness(graphSize:Long)(pathsCount:Long,distanceSum:Double,normalize:Boolean):Double={
    if(distanceSum==0) 0d else if(!normalize)  1d/(distanceSum) else (graphSize-1) /distanceSum
  }

  def standardClosenessValueMapper(pathSize:Double):Double={
    pathSize
  }

  def harmonicClosenessValueMapper(pathSize:Double):Double={
   if(pathSize==0d) 0d else  1d / pathSize
  }

  def harmonicCloseness(graphSize:Long)(pathsCount:Long,distanceSum:Double,normalize:Boolean):Double={
    if(!normalize) distanceSum else distanceSum/(graphSize-1)
  }

}


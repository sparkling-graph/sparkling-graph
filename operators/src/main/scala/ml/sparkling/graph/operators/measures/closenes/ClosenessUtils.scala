package ml.sparkling.graph.operators.measures.closenes

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object ClosenessUtils {

  type ClosenessFunction=(Long,Double)=>Double

  def standardCloseness(graphSize:Long)(pathsCount:Long,distanceSum:Double):Double={
    if(distanceSum==0) 0 else (pathsCount*pathsCount)/(distanceSum*(graphSize-1))
  }

  def harmonicCloseness(graphSize:Long)(pathsCount:Long,distanceSum:Double):Double={
    (pathsCount*pathsCount)/(Math.pow(2,distanceSum)*graphSize)
  }

}

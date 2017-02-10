package ml.sparkling.graph.operators.measures.vertex.hits

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object HitsUtils {
type ContinuePredicate=(Long,(Double,Double),(Double,Double))=>Boolean

  def convergenceAndIterationPredicate(delta:Double,maxIteration:Long=100)(iteration:Long, oldValues:(Double,Double), newValues:(Double,Double))={
    (oldValues,newValues) match{
      case ((hub1,_),(hub2,_)) =>Math.abs(hub1-hub2)>delta && iteration<maxIteration
    }

  }
}

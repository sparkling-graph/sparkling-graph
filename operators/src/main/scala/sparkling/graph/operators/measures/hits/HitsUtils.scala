package sparkling.graph.operators.measures.hits

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object HitsUtils {
type ContinuePredicate=(Long,(Double,Double),(Double,Double))=>Boolean

  def convergencePredicate(delta:Double)(iteration:Long,oldValues:(Double,Double),newValues:(Double,Double))={
    Math.abs(newValues._1-oldValues._1)>delta
  }
}

package ml.sparkling.graph.operators.measures.eigenvector

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object EigenvectorUtils {
  type ContinuePredicate=(Long,Double,Double)=>Boolean

  def convergencePredicate(delta:Double)(iteration:Long,oldValue:Double,newValue:Double)={
    Math.abs(newValue-oldValue)>delta
  }
}

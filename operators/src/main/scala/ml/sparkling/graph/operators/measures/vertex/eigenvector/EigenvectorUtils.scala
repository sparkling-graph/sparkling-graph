package ml.sparkling.graph.operators.measures.vertex.eigenvector

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object EigenvectorUtils {
  type ContinuePredicate=(Long,Double,Double)=>Boolean

  def convergenceAndIterationPredicate(delta:Double, maxIter:Long=100)(iteration:Long, oldValue:Double, newValue:Double)={
    Math.abs(newValue-oldValue)>delta && iteration<maxIter
  }
}

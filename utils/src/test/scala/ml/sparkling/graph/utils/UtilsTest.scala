package ml.sparkling.graph.utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.scalatest._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
abstract class UtilsTest(implicit sc:SparkContext)  extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers{
  def loadGraph(file:String)={
    GraphLoader.edgeListFile(sc,file.toString).cache()
  }

}
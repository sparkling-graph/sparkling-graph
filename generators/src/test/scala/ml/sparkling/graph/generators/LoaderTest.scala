package ml.sparkling.graph.generators

import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.scalatest._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
abstract class GeneratorTest(implicit sc:SparkContext)  extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers{

}
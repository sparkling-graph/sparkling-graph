package ml.sparkling.graph.generators

import ml.sparkling.graph.generators.ring.RingGenerator$Test
import ml.sparkling.graph.generators.wattsandstrogatz.WattsAndStrogatzGenerator$Test
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class SparkTest extends Spec with BeforeAndAfterAll {
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = true
  val master = "local[*]"

  def appName: String = "generators-tests"

  implicit val sc: SparkContext = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.ui.enabled","false")
    new SparkContext(conf)
  }

  override def afterAll() = {
    if(!sc.isStopped){
      sc.stop()
    }
  }

  override def nestedSuites = {
    Vector(
      new RingGenerator$Test,
      new WattsAndStrogatzGenerator$Test
    )
  }


}
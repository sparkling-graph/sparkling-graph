package ml.sparkling.graph.generators

import ml.sparkling.graph.generators.ring.RingGenerator$Test
import ml.sparkling.graph.generators.wattsandstrogatz.WatssAndStrogatzGenerator$Test
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class SparkTest extends Spec with BeforeAndAfterAll {

  val master = "local[*]"

  def appName: String = "generators-tests"

  implicit val sc: SparkContext = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    new SparkContext(conf)
  }

  override def afterAll() = {
    sc.stop()
  }

  override def nestedSuites = {
    Vector(
      new RingGenerator$Test,
      new WatssAndStrogatzGenerator$Test
    )
  }


}
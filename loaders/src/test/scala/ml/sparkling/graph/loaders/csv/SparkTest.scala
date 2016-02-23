package ml.sparkling.graph.loaders.csv


import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class SparkTest extends Suite with BeforeAndAfterAll {

  val master = "local[*]"

  def appName: String = "operators-tests"

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
     new CSVLoader$Test,
    new GraphFromCsv$Test
    )
  }


}
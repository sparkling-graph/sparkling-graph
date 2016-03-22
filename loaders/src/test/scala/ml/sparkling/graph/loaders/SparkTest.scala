package ml.sparkling.graph.loaders

import ml.sparkling.graph.loaders.csv.{CSVLoader$Test, GraphFromCsv$Test}
import ml.sparkling.graph.loaders.graphml.GraphFromGraphML$Test
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class SparkTest extends Spec with BeforeAndAfterAll {

  val master = "local[1]"

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
//     new CSVLoader$Test,
//     new GraphFromCsv$Test,
      new GraphFromGraphML$Test
    )
  }


}
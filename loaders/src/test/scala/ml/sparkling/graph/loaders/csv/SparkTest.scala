package ml.sparkling.graph.loaders.csv

import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, GivenWhenThen, Matchers}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
abstract class SparkTest  extends FlatSpec with BeforeAndAfter with GivenWhenThen with Matchers {

  val master = "local[8]"
  def appName:String

  implicit var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  def loadGraph(file:String)={
    GraphLoader.edgeListFile(sc,file.toString).partitionBy(PartitionStrategy.EdgePartition2D,16).cache()
  }

}
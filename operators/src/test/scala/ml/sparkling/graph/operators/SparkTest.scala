package ml.sparkling.graph.operators

import ml.sparkling.graph.operators.measures.clustering.LocalClustering
import org.apache.spark.graphx.{PartitionStrategy, GraphLoader, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{Matchers, GivenWhenThen, BeforeAndAfter, FlatSpec}

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
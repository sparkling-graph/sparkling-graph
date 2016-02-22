package ml.sparkling.graph.api.operators

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, GivenWhenThen, FlatSpec}
import org.mockito.Mockito._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class IterativeComputation$Test extends FlatSpec with BeforeAndAfter{

  val master = "local[*]"
  def appName:String="InterativeComputationTest"

  implicit var sc:SparkContext=None.orNull
  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }

  after {
    sc.stop()
  }

  def loadGraph(file:String)={
    GraphLoader.edgeListFile(sc,file.toString)
  }

  "Correct number of vertices " should "be returned" in{
    //Given("Graph")
    val graph=loadGraph(getClass.getResource("/graph").toString)
    //When("Taking size")
    val bucketSize: Long = IterativeComputation.wholeGraphBucket(graph)
    //Then("")
    assert(graph.numVertices==bucketSize)
  }

}

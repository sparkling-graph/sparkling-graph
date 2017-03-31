package ml.sparkling.graph.api.operators

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class IterativeComputation$Test extends FlatSpec with BeforeAndAfter{

  val master = "local[*]"
  def appName:String="InterativeComputationTest"

  implicit val sc:SparkContext= {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    new SparkContext(conf)
  }

  after {
    if(!sc.isStopped){
      sc.stop()
    }
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

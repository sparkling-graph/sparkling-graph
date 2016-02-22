package ml.sparkling.graph.loaders.csv

import ml.sparkling.graph.loaders.csv.providers.PropertyProviders
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec, GivenWhenThen, Matchers}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class CSVLoader$Test extends FlatSpec with BeforeAndAfter with GivenWhenThen with Matchers {

  private val master = "local[2]"
  private val appName = "example-spark"

  private implicit var sc: SparkContext = _

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

  "Simple csv file " should "be loaded and edges should have default attribute 1" in {
    Given("file path")
    val filePath = getClass.getResource("/simple.csv")
    When("Loads graph")
    val graph = CSVLoader.loadGraphFromCSV(filePath.toString)
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(6)
    graph.edges.count() should equal(5)
    graph.edges.collect().map(_.attr) should equal((0 until 5).map(x => 1))
  }


  "File that need vertices indexing" should "be loaded" in {
    Given("complex file path")
    val filePath = getClass.getResource("/longIDs.csv")
    When("Loads graph")
    val graph = CSVLoader.loadGraphFromCSVWitVertexIndexing(filePath.toString)
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(6)
    graph.edges.count() should equal(4)
    graph.vertices.collect().map{case (vId,data)=> vId}.sorted should equal(0 until 6)
  }

  "File that need vertices indexing and has not standard columns arragement" should "be loaded" in {
    Given("complex file path")
    val filePath = getClass.getResource("/multiColumn.csv")
    When("Loads graph")
    val graph = CSVLoader.loadGraphFromCSVWitVertexIndexing[String,Double](filePath.toString,column1=1,column2=3)
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(3)
    graph.edges.count() should equal(3)
    graph.vertices.collect().map{case (vId,data)=> data}.sorted should equal(List("adam", "marcin", "tomek"))
  }


  "Given column with long value" should "be extracted as edge attribute" in {
    Given("complex file path")
    val filePath = getClass.getResource("/multiColumn.csv")
    When("Loads graph")
    val graph = CSVLoader.loadGraphFromCSVWitVertexIndexing(filePath.toString,column1=1,column2=3,edgeAttributeProvider = PropertyProviders.longAttributeProvider(5) _)
    Then("Attribute should be extracted correctly")
    graph.edges.collect().map(_.attr).sorted should equal(List(15,32,56))
  }

  "Given column with double value" should "be extracted as edge attribute" in {
    Given("complex file path")
    val filePath = getClass.getResource("/multiColumn.csv")
    When("Loads graph")
    val graph = CSVLoader.loadGraphFromCSVWitVertexIndexing(filePath.toString,column1=1,column2=3,edgeAttributeProvider = PropertyProviders.doubleAttributeProvider(6) _)
    Then("Attribute should be extracted correctly")
    graph.edges.collect().map(_.attr).sorted should equal(List(1.25,1.34,1.58))
  }


}

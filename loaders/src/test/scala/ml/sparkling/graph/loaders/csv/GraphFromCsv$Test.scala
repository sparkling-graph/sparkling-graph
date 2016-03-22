package ml.sparkling.graph.loaders.csv

import ml.sparkling.graph.api.loaders.GraphLoading.{Parameter, LoadGraph}
import ml.sparkling.graph.loaders.LoaderTest
import ml.sparkling.graph.loaders.csv.DummyEdgeValue
import ml.sparkling.graph.loaders.csv.GraphFromCsv.CSV
import ml.sparkling.graph.loaders.csv.GraphFromCsv.LoaderParameters.{Indexing, EdgeValue, NoHeader}
import org.apache.spark.SparkContext


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class GraphFromCsv$Test(implicit sc:SparkContext)  extends LoaderTest {

  "CSV with standard format" should "be loaded by default" in{
    Given("CSV path")
    val filePath = getClass.getResource("/simple.csv").toString
    When("Loads graph")
    val graph = LoadGraph.from(CSV(filePath)).load()
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(6)
    graph.edges.count() should equal(5)
  }

  "CSV with standard format" should "be loaded with Integer as default edge" in{
    Given("CSV path")
    val filePath = getClass.getResource("/simple.csv").toString
    When("Loads graph")
    val graph = LoadGraph.from(CSV(filePath)).load[Nothing,Integer]()
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(6)
    graph.edges.count() should equal(5)
    graph.edges.collect().map(edge => edge.attr) should equal((0 until 5).map(x => 1))
  }


  "CSV with standard format" should "be loaded with Float as default edge" in{
    Given("CSV path")
    val filePath = getClass.getResource("/simple.csv").toString
    When("Loads graph")
    val graph = LoadGraph.from(CSV(filePath)).load[Nothing,Float]()
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(6)
    graph.edges.count() should equal(5)
    graph.edges.collect().map(edge => edge.attr) should equal((0 until 5).map(x => 1f))
  }


  "CSV with custom edge value" should "be loaded" in{
    Given("CSV path and type")
    val filePath = getClass.getResource("/simple.csv").toString
    When("Loads graph")
    val graph = LoadGraph.from(CSV(filePath)).using(EdgeValue(DummyEdgeValue)).load[Nothing,Any]()
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(6)
    graph.edges.count() should equal(5)
    graph.edges.collect().map(edge => edge.attr) should equal((0 until 5).map(x => DummyEdgeValue))
  }

  "CSV with standard format and no header" should "be loaded" in{
    Given("CSV path")
    val filePath = getClass.getResource("/simple_without_header.csv").toString
    When("Loads graph")
    val graph = LoadGraph.from(CSV(filePath)).using(NoHeader).load()
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(6)
    graph.edges.count() should equal(5)
  }

  "File that need vertices indexing" should "be loaded" in {
    Given("complex file path")
    val filePath = getClass.getResource("/longIDs.csv")
    When("Loads graph")
    val graph = LoadGraph.from(CSV(filePath.toString)).using(Indexing).load()
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(6)
    graph.edges.count() should equal(4)
    graph.vertices.collect().map{case (vId,data)=> vId}.distinct.size should equal(6)
  }


}

package ml.sparkling.graph.loaders.csv

import ml.sparkling.graph.api.loaders.GraphLoading.{Parameter, LoadGraph}
import ml.sparkling.graph.loaders.csv.GraphFromCsv.CSV
import ml.sparkling.graph.loaders.csv.GraphFromCsv.LoaderParameters.{EdgeValue, NoHeader}
import org.apache.spark.SparkContext


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class GraphFromCsv$Test(implicit sc:SparkContext)  extends MeasureTest {

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
    val graph = LoadGraph.from(CSV(filePath)).using(EdgeValue(NoHeader)).load[Nothing,Parameter]()
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(6)
    graph.edges.count() should equal(5)
    graph.edges.collect().map(edge => edge.attr) should equal((0 until 5).map(x => NoHeader))
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
}

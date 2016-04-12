package ml.sparkling.graph.loaders.graphml

import ml.sparkling.graph.api.loaders.GraphLoading.LoadGraph
import ml.sparkling.graph.loaders.LoaderTest
import ml.sparkling.graph.loaders.graphml.GraphFromGraphML.{GraphML, GraphProperties}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class GraphFromGraphML$Test(implicit sc:SparkContext)  extends LoaderTest {

  "GraphML with standard format" should "be loaded by default" in{
    Given("XML in GraphML format  path")
    val filePath = getClass.getResource("/simpleGraphML.xml").toString
    When("Loads graph")
    val graph = LoadGraph.from(GraphML(filePath)).load()
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(2)
    graph.edges.count() should equal(1)
  }

  "GraphML with standard format and multiple edges" should "be loaded by default" in{
    Given("XML in GraphML format path")
    val filePath = getClass.getResource("/simpleGraphML2.xml").toString
    When("Loads graph")
    val graph = LoadGraph.from(GraphML(filePath)).load()
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(3)
    graph.edges.count() should equal(2)
  }


  "GraphML with vertices attributes" should "be loaded by default" in{
    Given("XML in GraphML format  path")
    val filePath = getClass.getResource("/withValuesGraphML.xml").toString
    When("Loads graph")
    val graph: Graph[GraphProperties, GraphProperties] = LoadGraph.from(GraphML(filePath)).load()
    Then("Graph should be loaded correctly")
    graph.vertices.count() should equal(4)
    graph.edges.count() should equal(2)
    graph.vertices.map{
      case (vId,properites)=>(vId,properites("name").asInstanceOf[String])
    }.collect().sorted should equal(List((0l,"name0"),(1l,"name1"),(2l,"name2"),(3l,"name3")))
    graph.vertices.flatMap{
      case (vId,properites)=>properites.get("type").asInstanceOf[Option[String]].map((vId,_))
    }.collect().sorted should equal(List((0l,"type0")))
  }


}

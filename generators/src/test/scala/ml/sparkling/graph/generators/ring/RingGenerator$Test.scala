package ml.sparkling.graph.generators.ring

import ml.sparkling.graph.generators.GeneratorTest
import org.apache.spark.SparkContext

/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 26.04.16.
  */
class RingGenerator$Test(implicit val ctx:SparkContext) extends GeneratorTest {


  "Correct directed ring" should "be generated" in {
    Given("Generator configuration")
    val numberOfNodes: Long = 4
    val config = RingGeneratorConfiguration(numberOfNodes, false)
    When("Generates ring")
    val graph = RingGenerator.generate(config)
    Then("Graph should be generated correctly")
    graph.vertices.count() should equal(numberOfNodes)
    graph.edges.count() should equal(numberOfNodes)
    graph.edges.sortBy(_.srcId).map(e=>(e.srcId,e.dstId)).collect() should equal(Array((0,1),(1,2),(2,3),(3,0)))
  }


  "Correct undirected ring" should "be generated" in {
    Given("Generator configuration")
    val numberOfNodes: Long = 4
    val config = RingGeneratorConfiguration(numberOfNodes, true)
    When("Generates ring")
    val graph = RingGenerator.generate(config)
    Then("Graph should be generatedCorrectly")
    graph.vertices.count() should equal(numberOfNodes)
    graph.edges.count() should equal(2*numberOfNodes)
    graph.edges.sortBy(e=>(e.srcId,e.dstId)).map(e=>(e.srcId,e.dstId)).collect() should equal(Array((0,1),(0,3),(1,0),(1,2),(2,1),(2,3),(3,0),(3,2)))
  }


}

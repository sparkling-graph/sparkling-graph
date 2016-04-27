package ml.sparkling.graph.generators.wattsandstrogatz

import ml.sparkling.graph.generators.GeneratorTest
import ml.sparkling.graph.generators.ring.{RingGenerator, RingGeneratorConfiguration}
import org.apache.spark.SparkContext
import org.scalatest.FunSuite

/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 27.04.16.
  */
class WattsAndStrogatzGenerator$Test(implicit val ctx:SparkContext) extends GeneratorTest {


  "Correct directed ring with appropriate mean degree" should "be generated when probability of rewiring is 0" in {
    Given("Generator configuration")
    val numberOfNodes: Long = 10
    val meanDegree: Long = 2
    val rewiringProbability=0
    val config = WattsAndStrogatzGeneratorConfiguration(numberOfNodes, meanDegree,rewiringProbability)
    When("Generates graph")
    val graph = WattsAndStrogatzGenerator.generate(config)
    Then("Graph should be generated correctly")
    graph.vertices.count() should equal(numberOfNodes)
    graph.edges.count() should equal(numberOfNodes*meanDegree)
    graph.outDegrees.values.sum()/numberOfNodes should equal(meanDegree)
  }



  "Correct directed graph with appropriate mean degree" should "be generated when probability of rewiring is not 0" in {
    Given("Generator configuration")
    val numberOfNodes = 10
    val meanDegree = 2
    val rewiringProbability=0.9
    val config = WattsAndStrogatzGeneratorConfiguration(numberOfNodes, meanDegree,rewiringProbability)
    When("Generates graph")
    val graph = WattsAndStrogatzGenerator.generate(config)
    Then("Graph should be generated correctly")
    graph.vertices.count() should equal(numberOfNodes)
    graph.edges.count() should equal(numberOfNodes*meanDegree)
    val edges = graph.edges.map(e => (e.srcId, e.dstId)).sortByKey().collect()
    edges.toSet should  not equal  ( (0 to numberOfNodes-1).flatMap(n=>Iterator((n,(n-1+numberOfNodes)%numberOfNodes),(n,(n+1+numberOfNodes)%numberOfNodes))).toSet)
    graph.outDegrees.values.sum()/numberOfNodes should be(meanDegree.toDouble +- 1e-5)
  }

}

package ml.sparkling.graph.operators.measures.vertex.closenes

import java.io.File
import java.nio.file.Files

import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.commons.io.FileUtils

/**
  * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
  */
class Closeness$Test(implicit sc: SparkContext) extends MeasureTest {
  val tempDir = Files.createTempDirectory("spark-checkpoint")

  override def beforeAll() = {
    sc.setCheckpointDir(tempDir.toAbsolutePath.toString)
  }

  override def afterAll() = {
    FileUtils.deleteDirectory(tempDir.toFile)
  }


  "Closeness  for line graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes closeness")
    val result = Closeness.compute(graph)
    Then("Should calculate closeness correctly")
    val verticesSortedById = result.vertices.collect().sortBy { case (vId, data) => vId }
    verticesSortedById.map { case (vId, data) => data }.zip(Array(
      0.1, 1 / 6d, 1d / 3, 1, 0d
    )).foreach { case (a, b) => {
      a should be(b +- 1e-5)
    }
    }
  }


  "Closeness  for line graph" should "be correctly calculated using DSL" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes closeness")
    val result = graph.closenessCentrality()
    Then("Should calculate closeness correctly")
    val verticesSortedById = result.vertices.collect().sortBy { case (vId, data) => vId }
    verticesSortedById.map { case (vId, data) => data }.zip(Array(
      0.1, 1 / 6d, 1d / 3, 1, 0d
    )).foreach { case (a, b) => {
      a should be(b +- 1e-5)
    }
    }
  }




  "Closeness for full directed graph " should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes Closeness")
    val result = Closeness.compute(graph)
    Then("Should calculate Closeness correctly")
    val verticesSortedById = result.vertices.collect().sortBy { case (vId, data) => vId }
    verticesSortedById.map { case (vId, data) => data }.zip(Array(
      0.25, 1 / 6d, 0.2, 0.25
    )).foreach { case (a, b) => {
      a should be(b +- 1e-5)
    }
    }
  }

  "Closeness for full undirected graph " should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes Closeness")
    val result = Closeness.compute(graph, VertexMeasureConfiguration[Int, Int](true))
    Then("Should calculate Closeness correctly")
    val verticesSortedById = result.vertices.collect().sortBy { case (vId, data) => vId }
    verticesSortedById.map { case (vId, data) => data }.zip(Array(
      1 / 3d, 1 / 3d, 1 / 3d, 1 / 3d
    )).foreach { case (a, b) => {
      a should be(b +- 1e-5)
    }
    }
  }


  "Closeness for graph with uncontinous vertex ids space " should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/long_id_graph.csv")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes Closeness")
    val result = Closeness.compute(graph, VertexMeasureConfiguration[Int, Int](true))
    Then("Should calculate Closeness correctly")
    val verticesSortedById = result.vertices.collect().sortBy { case (vId, data) => vId }
    verticesSortedById.map { case (vId, data) => data }.zip(Array(
      1 / 6d, 0.25, 1 / 6d, 0.25
    )).foreach { case (a, b) => {
      a should be(b +- 1e-5)
    }
    }
  }


}
package ml.sparkling.graph.operators.measures.clustering

import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.SparkTest
import org.apache.spark.graphx.{GraphLoader, Graph}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class LocalClustering$Test  extends SparkTest{

  def appName = "local-clustering-test"


  "Local clustering for line graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=GraphLoader.edgeListFile(sc,filePath.toString).cache()
    When("Computes local clustering")
    val localClustering=LocalClustering.compute(graph)
    Then("Should calculate local clustering correctly")
    localClustering.vertices.collect().sortBy(t=>t._1) should equal (Array(
      (1,0.0), (2,0.0), (3,0.0), (4,0.0), (5,0.0)
    ))
  }

  "Local clustering for full directed graph " should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=GraphLoader.edgeListFile(sc,filePath.toString).cache()
    When("Computes local clustering")
    val localClustering=LocalClustering.compute(graph)
    Then("Should calculate local clustering correctly")
    localClustering.vertices.collect().sortBy(t=>t._1) should equal (Array(
      (1,0.5), (2,0d), (3,0d), (4,0.5)
    ))
  }

  "Local clustering for full undirected graph " should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=GraphLoader.edgeListFile(sc,filePath.toString).cache()
    When("Computes local clustering")
    val localClustering=LocalClustering.compute(graph,VertexMeasureConfiguration[Int,Int](true))
    Then("Should calculate local clustering correctly")
    localClustering.vertices.collect().sortBy(t=>t._1) should equal (Array(
      (1,1), (2,1), (3,1), (4,1)
    ))
  }


  "Local clustering for full directed graph " should "be correctly calculated using iterative approach" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=GraphLoader.edgeListFile(sc,filePath.toString).cache()
    When("Computes local clustering")
    val localClustering=LocalClustering.compute(graph)
    val localClusteringIterative=LocalClustering.compute(graph,VertexMeasureConfiguration[Int,Int]((g:Graph[Int,Int])=>1l))
    Then("Should calculate local clustering correctly")
    localClustering.vertices.collect().sortBy(t=>t._1) should equal (localClusteringIterative.vertices.collect().sortBy(t=>t._1))
  }

}
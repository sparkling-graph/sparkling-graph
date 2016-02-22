package ml.sparkling.graph.operators.algorithms.shortestpaths

import it.unimi.dsi.fastutil.longs.{Long2DoubleMap, Long2DoubleMaps}
import ml.sparkling.graph.operators.SparkTest
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{VertexId, Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import scala.collection.JavaConversions._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class ShortestPathsAlgorithm$Test  extends SparkTest{

  def  appName = "shortest-paths-test"


    "Shortest paths for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=GraphLoader.edgeListFile(sc,filePath.toString)
    When("Loads graph")
    val shortestPaths=ShortestPathsAlgorithm.computeShortestPaths(graph)
    Then("Should calculate shortest paths correctly")
      val verticesSortedById=shortestPaths.vertices.collect().sortBy(t=>t._1)
      verticesSortedById.map(t=>(t._1,t._2.mapValues(s=>s.map(l=>l.toList)))) should equal (Array(
      (1,Map(2 -> Set(List(1d)), 3 -> Set(List(2d,2d)), 4 -> Set(List(3d,3d,2d)), 5 -> Set(List(4d,4d,3d,2d)))),
      (2,Map(3 -> Set(List(1d)), 4 -> Set(List(2d,3d)), 5 -> Set(List(3d,4d,3d)) )),
      (3,Map(4 -> Set(List(1d)), 5 -> Set(List(2d,4d)))),
      (4,Map(5 -> Set(List(1d)))),
      (5,Map())
    ))
  }

  "Single shortest paths 1 for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=GraphLoader.edgeListFile(sc,filePath.toString)
    When("Loads graph")
    val shortestPaths=ShortestPathsAlgorithm.computeSingleShortestPathsLengths(graph,1)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy(t=>t._1)
    verticesSortedById should equal (Array(
      (1,0),
      (2,0),
      (3,0),
      (4,0),
      (5,0)
    ))
  }

  "Single shortest paths 2 for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=GraphLoader.edgeListFile(sc,filePath.toString)
    When("Loads graph")
    val shortestPaths=ShortestPathsAlgorithm.computeSingleShortestPathsLengths(graph,2l)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy(t=>t._1)
    verticesSortedById should equal (Array(
      (1,1),
      (2,0),
      (3,0),
      (4,0),
      (5,0)
    ))
  }



  "Undirected graphs" should "be handled correctly" in{
    Given("graph")
    val filePathDirected = getClass.getResource("/graphs/5_nodes_directed")
    val filePathUndirected = getClass.getResource("/graphs/5_nodes_undirected")
    val grapDirected=GraphLoader.edgeListFile(sc,filePathDirected.toString)
    val graphUndirected=GraphLoader.edgeListFile(sc,filePathUndirected.toString)
    When("Loads graph")
    val shortestPathsAsUndirected=ShortestPathsAlgorithm.computeShortestPaths(grapDirected,treatAsUndirected = true)
    val shortestPathsUndirected=ShortestPathsAlgorithm.computeShortestPaths(graphUndirected)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPathsAsUndirected.vertices.collect().sortBy(_._1)
    verticesSortedById should equal (shortestPathsUndirected.vertices.collect().sortBy(_._1))
  }

}

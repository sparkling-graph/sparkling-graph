package ml.sparkling.graph.operators.algorithms.shortestpaths

import ml.sparkling.graph.operators.MeasureTest
import ml.sparkling.graph.operators.algorithms.aproximation.ApproximatedShortestPathsAlgorithm
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance.DataMap
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.util.GraphGenerators
import org.scalatest.tagobjects.Slow

import scala.collection.JavaConversions._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class ShortestPathsAlgorithm$Test(implicit sc:SparkContext)   extends MeasureTest  {

  "Shortest paths for simple grid graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/9_nodes_grid")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString).cache()
    When("Computes shortest paths")
    val shortestPaths = ShortestPathsAlgorithm.computeShortestPaths(graph, treatAsUndirected = true)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById = shortestPaths.vertices.collect().sortBy { case (vId, _) => vId }
    verticesSortedById.map { case (vId, data) => (vId, data.mapValues(s => s.map(l => l.toList))) }.toSet should equal(Set(
      (1,Map(3 -> Set(List(2.0, 3.0, 2.0)), 2 -> Set(List(1.0, 2.0)), 5 -> Set(List(2.0, 5.0, 4.0), List(2.0, 5.0, 2.0)), 9 -> Set(List(4.0, 9.0, 6.0, 5.0, 2.0), List(4.0, 9.0, 6.0, 3.0, 2.0), List(4.0, 9.0, 8.0, 7.0, 4.0), List(4.0, 9.0, 8.0, 5.0, 2.0), List(4.0, 9.0, 8.0, 5.0, 4.0), List(4.0, 9.0, 6.0, 5.0, 4.0)), 6 -> Set(List(3.0, 6.0, 5.0, 4.0), List(3.0, 6.0, 3.0, 2.0), List(3.0, 6.0, 5.0, 2.0)), 1 -> Set(List(0.0)), 7 -> Set(List(2.0, 7.0, 4.0)), 8 -> Set(List(3.0, 8.0, 7.0, 4.0), List(3.0, 8.0, 5.0, 2.0), List(3.0, 8.0, 5.0, 4.0)), 4 -> Set(List(1.0, 4.0)))),
      (4,Map(3 -> Set(List(3.0, 3.0, 2.0, 1.0), List(3.0, 3.0, 6.0, 5.0), List(3.0, 3.0, 2.0, 5.0)), 2 -> Set(List(2.0, 2.0, 1.0), List(2.0, 2.0, 5.0)), 5 -> Set(List(1.0, 5.0)), 9 -> Set(List(3.0, 9.0, 8.0, 5.0), List(3.0, 9.0, 6.0, 5.0), List(3.0, 9.0, 8.0, 7.0)), 6 -> Set(List(2.0, 6.0, 5.0)), 1 -> Set(List(1.0, 1.0)), 7 -> Set(List(1.0, 7.0)), 8 -> Set(List(2.0, 8.0, 7.0), List(2.0, 8.0, 5.0)), 4 -> Set(List(0.0)))),
      (3,Map(3 -> Set(List(0.0)), 2 -> Set(List(1.0, 2.0)), 5 -> Set(List(2.0, 5.0, 6.0), List(2.0, 5.0, 2.0)), 9 -> Set(List(2.0, 9.0, 6.0)), 6 -> Set(List(1.0, 6.0)), 1 -> Set(List(2.0, 1.0, 2.0)), 7 -> Set(List(4.0, 7.0, 4.0, 1.0, 2.0), List(4.0, 7.0, 4.0, 5.0, 2.0), List(4.0, 7.0, 4.0, 5.0, 6.0), List(4.0, 7.0, 8.0, 9.0, 6.0), List(4.0, 7.0, 8.0, 5.0, 2.0), List(4.0, 7.0, 8.0, 5.0, 6.0)), 8 -> Set(List(3.0, 8.0, 5.0, 6.0), List(3.0, 8.0, 5.0, 2.0), List(3.0, 8.0, 9.0, 6.0)), 4 -> Set(List(3.0, 4.0, 5.0, 6.0), List(3.0, 4.0, 1.0, 2.0), List(3.0, 4.0, 5.0, 2.0)))),
      (5,Map(3 -> Set(List(2.0, 3.0, 2.0), List(2.0, 3.0, 6.0)), 2 -> Set(List(1.0, 2.0)), 5 -> Set(List(0.0)), 9 -> Set(List(2.0, 9.0, 8.0), List(2.0, 9.0, 6.0)), 6 -> Set(List(1.0, 6.0)), 1 -> Set(List(2.0, 1.0, 4.0), List(2.0, 1.0, 2.0)), 7 -> Set(List(2.0, 7.0, 8.0), List(2.0, 7.0, 4.0)), 8 -> Set(List(1.0, 8.0)), 4 -> Set(List(1.0, 4.0)))),
      (7,Map(3 -> Set(List(4.0, 3.0, 6.0, 9.0, 8.0), List(4.0, 3.0, 6.0, 5.0, 4.0), List(4.0, 3.0, 2.0, 5.0, 4.0), List(4.0, 3.0, 2.0, 5.0, 8.0), List(4.0, 3.0, 6.0, 5.0, 8.0), List(4.0, 3.0, 2.0, 1.0, 4.0)), 2 -> Set(List(3.0, 2.0, 5.0, 4.0), List(3.0, 2.0, 1.0, 4.0), List(3.0, 2.0, 5.0, 8.0)), 5 -> Set(List(2.0, 5.0, 8.0), List(2.0, 5.0, 4.0)), 9 -> Set(List(2.0, 9.0, 8.0)), 6 -> Set(List(3.0, 6.0, 5.0, 4.0), List(3.0, 6.0, 9.0, 8.0), List(3.0, 6.0, 5.0, 8.0)), 1 -> Set(List(2.0, 1.0, 4.0)), 7 -> Set(List(0.0)), 8 -> Set(List(1.0, 8.0)), 4 -> Set(List(1.0, 4.0)))),
      (2,Map(3 -> Set(List(1.0, 3.0)), 2 -> Set(List(0.0)), 5 -> Set(List(1.0, 5.0)), 9 -> Set(List(3.0, 9.0, 8.0, 5.0), List(3.0, 9.0, 6.0, 3.0), List(3.0, 9.0, 6.0, 5.0)), 6 -> Set(List(2.0, 6.0, 5.0), List(2.0, 6.0, 3.0)), 1 -> Set(List(1.0, 1.0)), 7 -> Set(List(3.0, 7.0, 4.0, 5.0), List(3.0, 7.0, 4.0, 1.0), List(3.0, 7.0, 8.0, 5.0)), 8 -> Set(List(2.0, 8.0, 5.0)), 4 -> Set(List(2.0, 4.0, 5.0), List(2.0, 4.0, 1.0)))),
      (6,Map(3 -> Set(List(1.0, 3.0)), 2 -> Set(List(2.0, 2.0, 3.0), List(2.0, 2.0, 5.0)), 5 -> Set(List(1.0, 5.0)), 9 -> Set(List(1.0, 9.0)), 6 -> Set(List(0.0)), 1 -> Set(List(3.0, 1.0, 2.0, 3.0), List(3.0, 1.0, 2.0, 5.0), List(3.0, 1.0, 4.0, 5.0)), 7 -> Set(List(3.0, 7.0, 4.0, 5.0), List(3.0, 7.0, 8.0, 9.0), List(3.0, 7.0, 8.0, 5.0)), 8 -> Set(List(2.0, 8.0, 9.0), List(2.0, 8.0, 5.0)), 4 -> Set(List(2.0, 4.0, 5.0)))),
      (8,Map(3 -> Set(List(3.0, 3.0, 6.0, 9.0), List(3.0, 3.0, 6.0, 5.0), List(3.0, 3.0, 2.0, 5.0)), 2 -> Set(List(2.0, 2.0, 5.0)), 5 -> Set(List(1.0, 5.0)), 9 -> Set(List(1.0, 9.0)), 6 -> Set(List(2.0, 6.0, 9.0), List(2.0, 6.0, 5.0)), 1 -> Set(List(3.0, 1.0, 2.0, 5.0), List(3.0, 1.0, 4.0, 7.0), List(3.0, 1.0, 4.0, 5.0)), 7 -> Set(List(1.0, 7.0)), 8 -> Set(List(0.0)), 4 -> Set(List(2.0, 4.0, 5.0), List(2.0, 4.0, 7.0)))),
      (9,Map(3 -> Set(List(2.0, 3.0, 6.0)), 2 -> Set(List(3.0, 2.0, 5.0, 8.0), List(3.0, 2.0, 3.0, 6.0), List(3.0, 2.0, 5.0, 6.0)), 5 -> Set(List(2.0, 5.0, 8.0), List(2.0, 5.0, 6.0)), 9 -> Set(List(0.0)), 6 -> Set(List(1.0, 6.0)), 1 -> Set(List(4.0, 1.0, 4.0, 5.0, 8.0), List(4.0, 1.0, 2.0, 5.0, 8.0), List(4.0, 1.0, 2.0, 3.0, 6.0), List(4.0, 1.0, 2.0, 5.0, 6.0), List(4.0, 1.0, 4.0, 7.0, 8.0), List(4.0, 1.0, 4.0, 5.0, 6.0)), 7 -> Set(List(2.0, 7.0, 8.0)), 8 -> Set(List(1.0, 8.0)), 4 -> Set(List(3.0, 4.0, 5.0, 6.0), List(3.0, 4.0, 5.0, 8.0), List(3.0, 4.0, 7.0, 8.0))))
    ))
    graph.unpersist(true)
  }

  "Shortest paths for simple ring graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_ring")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ShortestPathsAlgorithm.computeShortestPaths(graph,treatAsUndirected = true)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,_)=>vId}
    verticesSortedById.map{case (vId,data)=>(vId,data.mapValues(s=>s.map(l=>l.toList)))}.toSet should equal (Set(
      (3,Map(5 -> Set(List(2.0, 5.0, 4.0)), 1 -> Set(List(2.0, 1.0, 2.0)), 3 -> Set(List(0.0)), 4 -> Set(List(1.0, 4.0)), 2 -> Set(List(1.0, 2.0)))),
      (5,Map(5 -> Set(List(0.0)), 1 -> Set(List(1.0, 1.0)), 3 -> Set(List(2.0, 3.0, 4.0)), 4 -> Set(List(1.0, 4.0)), 2 -> Set(List(2.0, 2.0, 1.0)))),
      (4,Map(5 -> Set(List(1.0, 5.0)), 1 -> Set(List(2.0, 1.0, 5.0)), 3 -> Set(List(1.0, 3.0)), 4 -> Set(List(0.0)), 2 -> Set(List(2.0, 2.0, 3.0)))),
      (2,Map(5 -> Set(List(2.0, 5.0, 1.0)), 1 -> Set(List(1.0, 1.0)), 3 -> Set(List(1.0, 3.0)), 4 -> Set(List(2.0, 4.0, 3.0)), 2 -> Set(List(0.0)))),
      (1,Map(5 -> Set(List(1.0, 5.0)), 1 -> Set(List(0.0)), 3 -> Set(List(2.0, 3.0, 2.0)), 4 -> Set(List(2.0, 4.0, 5.0)), 2 -> Set(List(1.0, 2.0))))
    )
    )
    graph.unpersist(true)
  }


  "Shortest paths lengths for simple graph" should "be correctly calculated using iterative approach" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths =ShortestPathsAlgorithm.computeShortestPathsLengthsIterative(graph, (g:Graph[_,_])=>1)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,data)=>vId}.map{
      case (vId,data)=>(vId,data.toMap)
    }
    verticesSortedById  should equal (Array(
      (1,Map(1->0.0, 2 -> 1.0, 3 -> 2.0, 4 -> 3.0, 5 -> 4.0)),
      (2,Map(2->0.0, 3 -> 1.0, 4 -> 2.0, 5 -> 3.0 )),
      (3,Map(3->0.0, 4 -> 1.0, 5 -> 2.0)),
      (4,Map(4->0.0, 5 -> 1.0)),
      (5,Map(5->0.0))
    ))
    graph.unpersist(true)
  }

  "Single shortest paths lengths 1 for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ShortestPathsAlgorithm.computeSingleShortestPathsLengths(graph,1)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById should equal (Array(
      (1,0),
      (2,0),
      (3,0),
      (4,0),
      (5,0)
    ))
    graph.unpersist(true)
  }

  "Single shortest paths lengths for vertex 2  for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ShortestPathsAlgorithm.computeSingleShortestPathsLengths(graph,2l)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,_)=>vId}
    verticesSortedById should equal (Array(
      (1,1),
      (2,0),
      (3,0),
      (4,0),
      (5,0)
    ))
    graph.unpersist(true)
  }

  "Single shortest paths for small grid graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/9_nodes_grid")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ShortestPathsAlgorithm.computeShortestPathsLengths(graph,treatAsUndirected = true)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().sortBy{case (vId,data)=>vId}.map{
      case (vId,data)=>(vId,data.toMap)
    }
    verticesSortedById should equal (Array(
      (1,Map(1->0.0, 7->2.0, 8->3.0, 3->2.0, 5->2.0, 4->1.0, 9->4.0, 6->3.0, 2->1.0)),
      (2,Map(2->0.0, 7->3.0, 8->2.0, 3->1.0, 5->1.0, 4->2.0, 9->3.0, 6->2.0, 1->1.0)),
      (3,Map(3->0.0, 7->4.0, 8->3.0, 5->2.0, 4->3.0, 9->2.0, 6->1.0, 2->1.0, 1->2.0)),
      (4,Map(4->0.0, 7->1.0, 8->2.0, 3->3.0, 5->1.0, 9->3.0, 6->2.0, 2->2.0, 1->1.0)),
      (5,Map(5->0.0, 7->2.0, 8->1.0, 3->2.0, 9->2.0, 4->1.0, 6->1.0, 2->1.0, 1->2.0)),
      (6,Map(6->0.0, 7->3.0, 8->2.0, 3->1.0, 5->1.0, 4->2.0, 9->1.0, 2->2.0, 1->3.0)),
      (7,Map(7->0.0, 8->1.0, 3->4.0, 5->2.0, 9->2.0, 4->1.0, 6->3.0, 2->3.0, 1->2.0)),
      (8,Map(8->0.0, 7->1.0, 3->3.0, 5->1.0, 4->2.0, 9->1.0, 6->2.0, 2->2.0, 1->3.0)),
      (9,Map(9->0.0, 7->2.0, 8->1.0, 3->2.0, 5->2.0, 4->3.0, 6->1.0, 2->3.0, 1->4.0))))
    graph.unpersist(true)
  }

  " Our shortest paths for random RMAT graph " should "not take longer thant GraphX"  taggedAs(Slow) in{
    Given("graph")
    val size=700
    val meanDegree=32
    val graph=GraphGenerators.rmatGraph(sc,size,meanDegree*size).mapEdges(_=>1)
    graph.vertices.collect()
    graph.edges.collect()
    sc.parallelize((1 to 10000)).map(_*1000).treeReduce(_+_)
    When("Computes shortest paths")
    val (ourData,oursTime) =time("Ours shortest paths for RMAT graph")(ShortestPathsAlgorithm.computeShortestPathsLengths(graph))
    val (graphXData,graphxTime) =time("Graphx shortest paths  for RMAT graph")(ShortestPaths.run(graph,graph.vertices.collect().map(_._1).toList))

    Then("Approximation should be faster")
    oursTime should be <(graphxTime)
    (ourData.vertices.flatMap{
      case (id,data)=>data.toList.map{
        case (id2,dist)=>((id,id2),dist.toInt)
      }
    }.collect() ++ (graphXData.vertices.flatMap{
      case (id,data)=>data.toList.map{
        case (id2,dist)=>{
          ((id,id2),dist)
        }
      }
    }.collect())).groupBy(_._1).map{
      case (id,data)=>{
        withClue(id){data should have length(2)}
        (id,data(0)._2-data(1)._2)
      }
    }.filter(_._2 != 0) shouldBe empty
    graph.unpersist(true)
  }

  "Undirected graphs" should "be handled correctly" in{
    Given("graph")
    val filePathDirected = getClass.getResource("/graphs/5_nodes_directed")
    val filePathUndirected = getClass.getResource("/graphs/5_nodes_undirected")
    val grapDirected=loadGraph(filePathDirected.toString)
    val graphUndirected=loadGraph(filePathUndirected.toString)
    When("Loads graph")
    val shortestPathsAsUndirected=ShortestPathsAlgorithm.computeShortestPaths(grapDirected,treatAsUndirected = true)
    val shortestPathsUndirected=ShortestPathsAlgorithm.computeShortestPaths(graphUndirected)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPathsAsUndirected.vertices.collect().sortBy{case (vId,data)=>vId}
    verticesSortedById should equal (shortestPathsUndirected.vertices.collect().sortBy{case (vId,data)=>vId})
    grapDirected.unpersist(true)
    graphUndirected.unpersist(true)
  }

}

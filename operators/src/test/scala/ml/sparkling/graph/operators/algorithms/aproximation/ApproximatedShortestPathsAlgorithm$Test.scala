package ml.sparkling.graph.operators.algorithms.aproximation
import org.scalatest.tagobjects.Slow
import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import org.apache.log4j.Logger
import org.apache.spark.graphx.util.GraphGenerators

/**
  * Created by  Roman Bartusiak <riomus@gmail.com> on 07.02.17.
  */
class ApproximatedShortestPathsAlgorithm$Test(implicit sc:SparkContext)   extends MeasureTest  {
  def time[T](str: String)(thunk: => T): (T,Long) = {
    logger.info(s"$str...")
    val t1 = System.currentTimeMillis
    val x = thunk
    val t2 = System.currentTimeMillis
    val diff=t2 - t1
    logger.info(s"$diff ms")
    (x,diff)
  }

  "Approximated shortest paths for simple graph" should "be correctly calculated using iterative approach" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths =ApproximatedShortestPathsAlgorithm.computeShortestPathsLengthsIterative(graph, (g:Graph[_,_])=>1,treatAsUndirected = false)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().map{
      case (vId,data)=>(vId,data.toMap)
    }.toSet
    verticesSortedById  should equal (
      Set((1,Map(2 -> 1.0, 5 -> 8.0, 4 -> 5.0, 3 -> 2.0)), (3,Map(5 -> 2.0, 4 -> 1.0)), (5,Map()), (2,Map(5 -> 8.0, 4 -> 2.0, 3 -> 1.0)), (4,Map(5 -> 1.0)))
    )
    graph.unpersist(true)
  }

  "Approximated shortest paths for undirected ring graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_ring")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths =ApproximatedShortestPathsAlgorithm.computeShortestPathsLengthsIterative(graph, (g:Graph[_,_])=>1)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.collect().map{
      case (vId,data)=>(vId,data.toMap)
    }.toSet
    verticesSortedById  should equal (Set(
      (2,Map(5 -> 2.0, 4 -> 2.0, 1 -> 1.0, 3 -> 1.0)),
      (4,Map(2 -> 2.0, 5 -> 1.0, 1 -> 2.0, 3 -> 1.0)),
      (3,Map(2 -> 1.0, 5 -> 2.0, 4 -> 1.0, 1 -> 2.0)),
      (1,Map(2 -> 1.0, 5 -> 1.0, 4 -> 2.0, 3 -> 2.0)),
      (5,Map(2 -> 2.0, 4 -> 1.0, 1 -> 1.0, 3 -> 2.0))) )
    graph.unpersist(true)
  }

  "Single shortest paths 1 for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ApproximatedShortestPathsAlgorithm.computeSingleShortestPathsLengths(graph,1, treatAsUndirected = false)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.map{
      case (vId,data)=>(vId,data.toMap)
    }.collect().toSet
    verticesSortedById should equal (Set((1,Map()), (2,Map()), (3,Map()), (4,Map()), (5,Map())))
    graph.unpersist(true)
  }

  "Single shortest paths 2 for simple graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes shortest paths")
    val shortestPaths=ApproximatedShortestPathsAlgorithm.computeSingleShortestPathsLengths(graph,2l, treatAsUndirected = false)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPaths.vertices.map{
      case (vId,data)=>(vId,data.toMap)
    }.collect().toSet
    verticesSortedById should equal (Set(
      (1,Map(2->1)),
      (2,Map()),
      (3,Map()),
      (4,Map()),
      (5,Map())
    ))
    graph.unpersist(true)
  }

  "Undirected graphs" should "be handled correctly" in{
    Given("graph")
    val filePathDirected = getClass.getResource("/graphs/5_nodes_directed")
    val filePathUndirected = getClass.getResource("/graphs/5_nodes_undirected")
    val grapDirected=loadGraph(filePathDirected.toString)
    val graphUndirected=loadGraph(filePathUndirected.toString)
    When("Loads graph")
    val shortestPathsAsUndirected=ApproximatedShortestPathsAlgorithm.computeShortestPaths(grapDirected)
    val shortestPathsUndirected=ApproximatedShortestPathsAlgorithm.computeShortestPaths(graphUndirected,treatAsUndirected = false)
    Then("Should calculate shortest paths correctly")
    val verticesSortedById=shortestPathsAsUndirected.vertices.mapValues(_.toMap).collect().sortBy{case (vId,data)=>vId}
    verticesSortedById should equal (shortestPathsUndirected.vertices.mapValues(_.toMap).collect().sortBy{case (vId,data)=>vId})
    grapDirected.unpersist(true)
    graphUndirected.unpersist(true)
  }


   " Approximation for random RMAT graph " should "not take longer thant exact computing"  taggedAs(Slow) in{
    Given("graph")
    val graph=GraphGenerators.rmatGraph(sc,2000,40000)
    graph.vertices.collect()
    graph.edges.collect()
    When("Computes shortest paths")
    val (_,exactTime) =time("Exact shortest paths for RMAT graph")(ShortestPathsAlgorithm.computeShortestPathsLengths(graph))
    val (_,approximationTime) =time("Aproximated shortest paths  for grid graph")(ApproximatedShortestPathsAlgorithm.computeShortestPaths(graph ))

    Then("Approximation should be faster")
    approximationTime should be <=(exactTime)
     graph.unpersist(true)
  }

  " Approximation for random log normal graph " should  "not take longer thant exact computing"  taggedAs(Slow) in{
    Given("graph")
    val graph=GraphGenerators.logNormalGraph(sc,7000).cache()
    graph.vertices.collect()
    graph.edges.collect()
    sc.parallelize((1 to 10000)).map(_*1000).treeReduce(_+_)
    When("Computes shortest paths")
    val (_,exactTime) =time("Exact shortest paths for log normal graph")(ShortestPathsAlgorithm.computeShortestPathsLengths(graph, treatAsUndirected = true))
    val (_,approximationTime) =time("Aproximated shortest paths for log normal")(ApproximatedShortestPathsAlgorithm.computeShortestPaths(graph, treatAsUndirected = true ))

    Then("Approximation should be faster")
    approximationTime should be <=(exactTime)
    graph.unpersist(true)
  }


}

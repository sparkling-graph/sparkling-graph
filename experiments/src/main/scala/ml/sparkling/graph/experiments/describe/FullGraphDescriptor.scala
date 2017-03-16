package ml.sparkling.graph.experiments.describe

import ml.sparkling.graph.api.operators.IterativeComputation._
import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.Component
import ml.sparkling.graph.api.operators.measures.{VertexMeasure, VertexMeasureConfiguration}
import ml.sparkling.graph.operators.algorithms.aproximation.ApproximatedShortestPathsAlgorithm
import ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation.LPCoarsening
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN
import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import ml.sparkling.graph.operators.measures.vertex.{Degree, NeighborhoodConnectivity, VertexEmbeddedness}
import ml.sparkling.graph.operators.measures.vertex.closenes.Closeness
import ml.sparkling.graph.operators.measures.vertex.clustering.LocalClustering
import ml.sparkling.graph.operators.measures.vertex.eigenvector.EigenvectorCentrality
import ml.sparkling.graph.operators.measures.vertex.hits.Hits
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag


/**
  * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
  */
object FullGraphDescriptor {
  val logger = Logger.getLogger(FullGraphDescriptor.getClass)
  private val measures = List(
    ("Eigenvector", EigenvectorCentrality),
    ("Hits", Hits),
    ("NeighborConnectivity", NeighborhoodConnectivity),
    ("Degree", Degree),
    ("VertexEmbeddedness", VertexEmbeddedness),
    ("LocalClustering", LocalClustering),
    ("Label propagation coarsening", LPCoarsening),
    ("PSCAN", PSCAN),
    ("Closeness", Closeness),
    ("Shortest paths", ShortestPathsAlgorithm),
    ("Approximated shortest paths", ApproximatedShortestPathsAlgorithm)
  )
  type MeasureFilter=((String,Object))=>Boolean
  val anyMeasureFilter:MeasureFilter=(_)=>true;

  def time[T](str: String)(thunk: => T): (T, Long) = {
    logger.info(str + "... ")
    val t1 = System.currentTimeMillis
    val x = thunk
    val t2 = System.currentTimeMillis
    val diff = t2 - t1
    logger.info(diff + " msecs")
    (x, diff)
  }


  def describeGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED],measureFilter:MeasureFilter=anyMeasureFilter)(implicit num: Numeric[ED]) = {
    val cachedGraph = graph.cache()
    val outGraph: Graph[List[Any], ED] = cachedGraph.mapVertices((vId, data) => List(data))

    measures.filter(measureFilter).foldLeft(outGraph) {
      case (acc, (measureName, measure)) => {
        val graphMeasures = executeOperator(graph, vertexMeasureConfiguration, cachedGraph, measure, measureName)
        graphMeasures.unpersist()
        acc.joinVertices(graphMeasures.vertices)(extendValueList)
      }
    }
  }

  def executeOperator[ED: ClassTag, VD: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED], cachedGraph: Graph[VD, ED], measure: Object, name: String)(implicit num: Numeric[ED]) = {
    measure match {
      case m: VertexMeasure[Any@unchecked] => m.compute(cachedGraph, vertexMeasureConfiguration)
      case m@ShortestPathsAlgorithm =>  m.computeShortestPathsLengthsIterative(cachedGraph, vertexMeasureConfiguration.bucketSizeProvider,vertexMeasureConfiguration.treatAsUndirected)
      case m@ApproximatedShortestPathsAlgorithm => {
        val bucketSize= vertexMeasureConfiguration.bucketSizeProvider(graph);
        val bucketSizeProvider=(_:Graph[Component,ED])=>bucketSize;
        m.computeShortestPathsLengthsIterative(cachedGraph, bucketSizeProvider,vertexMeasureConfiguration.treatAsUndirected)
      }
      case m@LPCoarsening => m.coarse(cachedGraph, vertexMeasureConfiguration.treatAsUndirected)
      case m@PSCAN =>m.detectCommunities(graph)
    }
  }
  def executeOperatorToPath[ED: ClassTag, VD: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED], cachedGraph: Graph[VD, ED], measure: Object, name: String, outDirectory: String)(implicit num: Numeric[ED]) = {
    measure match {
      case m: VertexMeasure[Any@unchecked] => Some(m.compute(cachedGraph, vertexMeasureConfiguration))
      case m@ShortestPathsAlgorithm => {
        m.computeAPSPToDirectory(cachedGraph, outDirectory, vertexMeasureConfiguration.treatAsUndirected, vertexMeasureConfiguration.bucketSizeProvider(graph)); None
      }
      case m@ApproximatedShortestPathsAlgorithm => {
        m.computeAPSPToDirectory(cachedGraph, outDirectory, vertexMeasureConfiguration.treatAsUndirected, vertexMeasureConfiguration.bucketSizeProvider(graph)); None
      }
      case m@LPCoarsening => Some(m.coarse(cachedGraph, vertexMeasureConfiguration.treatAsUndirected))
      case m@PSCAN => Some(m.detectCommunities(graph))
    }
  }

  def describeGraphToDirectory[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], directory: String, vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED],measureFilter:MeasureFilter=anyMeasureFilter)(implicit num: Numeric[ED]):List[(String,Long)] = {
    val cachedGraph = graph.cache()
    val outGraph = cachedGraph.mapVertices((vId, data) => List(data)).cache()
    measures.filter(measureFilter).map { case (measureName, measure) => {
      val (_,timeResult)=time(measureName)({
        val graphMeasuresOpt = executeOperatorToPath(graph, vertexMeasureConfiguration, cachedGraph, measure, measureName, s"${directory}/${measureName}")
        graphMeasuresOpt match {
          case Some(graphMeasures) => {
            val outputCSV = outGraph.outerJoinVertices(graphMeasures.vertices)(extendValueList)
              .vertices.map {
              case (id, data) => s"${id};${data.reverse.mkString(";")}"
            }.cache()
            outputCSV.saveAsTextFile(s"${directory}/${measureName}")
            graphMeasures.unpersist()
          }
          case None => {
            logger.info("Operator persistance not needed");
          }
        }

      })
      (measureName,timeResult)
    }
    }
  }

  private def extendValueList(vId: VertexId, oldValue: List[Any], newValue: Any) = {
    newValue match {
      case None => oldValue
      case Some(v: String) => v :: oldValue
      case Some(v: Double) => v :: oldValue
      case Some(v: Int) => v :: oldValue
      case Some((v1: Int, v2: Int)) => v1 :: v2 :: oldValue
      case Some((v1: Double, v2: Double)) => v1 :: v2 :: oldValue
      case Some(component: List[_]) => component ++ oldValue
      case Some(any) => any :: oldValue
    }
  }
}

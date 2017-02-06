package ml.sparkling.graph.experiments.describe

import ml.sparkling.graph.api.operators.measures.{VertexMeasure, VertexMeasureConfiguration}
import ml.sparkling.graph.operators.measures.vertex.{NeighborhoodConnectivity, VertexEmbeddedness, Degree}
import ml.sparkling.graph.operators.measures.vertex.closenes.Closeness
import ml.sparkling.graph.operators.measures.vertex.clustering.LocalClustering
import ml.sparkling.graph.operators.measures.vertex.eigenvector.EigenvectorCentrality
import ml.sparkling.graph.operators.measures.vertex.hits.Hits
import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object FullGraphDescriptor {
  private val measures = List(
    ("Eigenvector", EigenvectorCentrality),
    ("Hits", Hits),
    ("NeighborConnectivity", NeighborhoodConnectivity),
    ("Degree", Degree),
    ("VertexEmbeddedness", VertexEmbeddedness),
    ("LocalClustering", LocalClustering),
    ("Closeness", Closeness)
  )

  def describeGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED])(implicit num: Numeric[ED]) = {
    val cachedGraph = graph.cache()
    val outGraph: Graph[List[Any], ED] = cachedGraph.mapVertices((vId, data) => List(data))

    measures.foldLeft(outGraph) {
      case (acc, (measureName, measure)) => {
        val graphMeasures = measure match {
          case m: VertexMeasure[Any@unchecked] => m.compute(cachedGraph, vertexMeasureConfiguration)
        }
        graphMeasures.unpersist()
        acc.joinVertices(graphMeasures.vertices)(extendValueList)
      }
    }
  }


  def describeGraphToDirectory[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], directory: String, vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED])(implicit num: Numeric[ED]) = {
    val cachedGraph = graph.cache()
    val outGraph: Graph[List[Any], ED] = cachedGraph.mapVertices((vId, data) => List(data))
    measures.foreach { case (measureName, measure) => {
      val graphMeasures = measure match {
        case m: VertexMeasure[Any@unchecked] => m.compute(cachedGraph, vertexMeasureConfiguration)
      }
      val outputCSV = outGraph.outerJoinVertices(graphMeasures.vertices)(extendValueList)
        .vertices.map {
        case (id, data) => s"${id};${data.reverse.mkString(";")}"
      }
      outputCSV.saveAsTextFile(s"${directory}/${measureName}")
      graphMeasures.unpersist()
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
    }
  }
}

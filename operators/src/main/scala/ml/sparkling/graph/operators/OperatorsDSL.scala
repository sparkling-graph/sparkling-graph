package ml.sparkling.graph.operators

import ml.sparkling.graph.api.operators.algorithms.community.CommunityDetection._
import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN._
import ml.sparkling.graph.operators.measures.edge.{CommonNeighbours, AdamicAdar}
import ml.sparkling.graph.operators.measures.vertex.{Degree, NeighborhoodConnectivity, VertexEmbeddedness}
import ml.sparkling.graph.operators.measures.vertex.clustering.LocalClustering
import ml.sparkling.graph.operators.measures.graph.{Modularity, FreemanCentrality}
import ml.sparkling.graph.operators.partitioning.CommunityBasedPartitioning._
import ml.sparkling.graph.operators.measures.vertex.closenes.Closeness
import ml.sparkling.graph.operators.measures.vertex.eigenvector.EigenvectorCentrality
import ml.sparkling.graph.operators.measures.vertex.hits.Hits
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object OperatorsDSL {

  implicit class ModularityDSL[E:ClassTag](graph:Graph[ComponentID,E]){
    def modularity()=Modularity.compute(graph)
  }

  implicit class DSL[VD:ClassTag ,ED:ClassTag](graph:Graph[VD,ED]){
    def PSCAN(epsilon:Double=0.1)=
      computeConnectedComponents(graph,epsilon)

    def closenessCentrality(vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED]=VertexMeasureConfiguration())(implicit num:Numeric[ED])=
      Closeness.compute(graph,vertexMeasureConfiguration)

    def eigenvectorCentrality(vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED]=VertexMeasureConfiguration())(implicit num:Numeric[ED])=
      EigenvectorCentrality.compute(graph,vertexMeasureConfiguration)

    def hits(vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED]=VertexMeasureConfiguration())(implicit num:Numeric[ED])=
      Hits.compute(graph,vertexMeasureConfiguration)

    def degreeCentrality(vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED]=VertexMeasureConfiguration())(implicit num:Numeric[ED])=
      Degree.compute(graph,vertexMeasureConfiguration)

    def neighborhoodConnectivity(vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED]=VertexMeasureConfiguration())(implicit num:Numeric[ED])=
      NeighborhoodConnectivity.compute(graph,vertexMeasureConfiguration)

    def vertexEmbeddedness(vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED]=VertexMeasureConfiguration())(implicit num:Numeric[ED])=
      VertexEmbeddedness.compute(graph,vertexMeasureConfiguration)

    def localClustering(vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED]=VertexMeasureConfiguration())(implicit num:Numeric[ED])=
      LocalClustering.compute(graph,vertexMeasureConfiguration)

    def freemanCentrality()=FreemanCentrality.compute(graph)

    def partitionBy(communityDetectionMethod:CommunityDetectionMethod[VD,ED])(implicit sc:SparkContext)=
      partitionGraphBy(graph,communityDetectionMethod)

    def partitionBy(communityDetectionMethod:CommunityDetectionAlgorithm)(implicit sc:SparkContext)=
      partitionGraphBy(graph,communityDetectionMethod)

    def adamicAdar(treatAsUndirected:Boolean=false)={
      AdamicAdar.computeWithPreprocessing(graph,treatAsUndirected)
    }

    def commonNeighbours(treatAsUndirected:Boolean=false)={
      CommonNeighbours.computeWithPreprocessing(graph,treatAsUndirected)
    }

  }
}

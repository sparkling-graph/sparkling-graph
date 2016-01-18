package sparkling.graph.examples

import org.apache.spark.graphx.{Graph, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}
import sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import sparkling.graph.loaders.csv.providers.PropertyProviders
import sparkling.graph.loaders.csv.{CSVLoader, CsvLoaderConfig}
import sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import sparkling.graph.operators.measures.VertexEmbeddedness
import sparkling.graph.operators.predicates.ByIdsPredicate
import sparkling.graph.experiments.describe.GraphDescriptor._
import sparkling.graph.operators.measures.clustering.LocalClustering

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */

object GraphDescriptionFromCSV extends ExampleApp {
  def body()={
  val configuration = if (bucketSize == -1l) {
    val graphSize=partitionedGraph.numVertices
    println(s"BUCKET SIZE WILL BE EQUAL TO GRAPH SIZE ${graphSize}!!")
    VertexMeasureConfiguration[String,Double](treatAsUndirected,(g:Graph[String,Double])=>graphSize)
  }
  else
    VertexMeasureConfiguration[String,Double](treatAsUndirected,(g:Graph[String,Double])=>bucketSize)
    val groupedGraph=partitionedGraph.groupEdges((a,b)=>a)
    groupedGraph.describeGraphToDirectory(out, configuration)
  ctx.stop()
  }
}

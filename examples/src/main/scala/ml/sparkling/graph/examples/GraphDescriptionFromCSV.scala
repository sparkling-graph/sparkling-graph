package ml.sparkling.graph.examples

import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.experiments.describe.GraphDescriptor._
import org.apache.log4j.Logger
import org.apache.spark.graphx.Graph

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */

object GraphDescriptionFromCSV extends ExampleApp {
  val logger=Logger.getLogger(GraphDescriptionFromCSV.getClass())
  def body()={
  val configuration = if (bucketSize == -1l) {
    val graphSize=1000l
    logger.info(s"BUCKET SIZE WILL BE EQUAL TO 1000!!")
    VertexMeasureConfiguration[String,Double](treatAsUndirected,(g:Graph[String,Double])=>graphSize)
  }
  else
    VertexMeasureConfiguration[String,Double](treatAsUndirected,(g:Graph[String,Double])=>bucketSize)
    val groupedGraph=partitionedGraph.groupEdges((a,b)=>a)
    groupedGraph.describeGraphToDirectory(out, configuration)
  ctx.stop()

  }
}

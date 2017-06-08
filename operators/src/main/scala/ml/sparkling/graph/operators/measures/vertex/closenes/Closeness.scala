package ml.sparkling.graph.operators.measures.vertex.closenes

import ml.sparkling.graph.api.operators.IterativeComputation
import ml.sparkling.graph.api.operators.measures.{VertexMeasure, VertexMeasureConfiguration}
import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import ml.sparkling.graph.operators.measures.vertex.closenes.ClosenessUtils._
import ml.sparkling.graph.operators.predicates.InArrayPredicate
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
  * Computes closeness centrality in standard and harmonic versions
  */
object Closeness extends VertexMeasure[Double] {

  val logger=Logger.getLogger(Closeness.getClass);
  /**
    * Generic closeness computation method, should be used for extensions. Computations are done using super-step approach
    *
    * @param graph                      - computation graph
    * @param closenessFunction          - function that calculates closeness for vertex
    * @param vertexMeasureConfiguration - configuration of computation
    * @param num                        - numeric for @ED
    * @tparam VD - vertex data type
    * @tparam ED - edge data type
    * @return graph where each vertex is associated with its  closeness centrality computed using @closenessFunction
    */
  def computeUsing[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                               closenessFunction: ClosenessFunction,
                                               pathMappingFunction: PathMappingFunction,
                                               vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED],
                                               normalize: Boolean = false,
                                               checkpointingFrequency: Int = 20)(implicit num: Numeric[ED]): Graph[Double, ED] = {
    val groupedVerticesIds = graph.vertices.map(_._1).treeAggregate(mutable.ListBuffer.empty[VertexId])(
      (agg:ListBuffer[VertexId],data:VertexId)=>{agg+=data;agg},
      (agg:ListBuffer[VertexId],agg2:ListBuffer[VertexId])=>{agg++=agg2;agg}
    ).grouped(vertexMeasureConfiguration.bucketSizeProvider(graph).toInt).toList.map(_.toArray)
    val numberOfIterations=groupedVerticesIds.size
    val distanceSumGraph = graph.mapVertices((vId, data) => (0l, 0d))
    graph.cache()
    val out=groupedVerticesIds.zipWithIndex.foldLeft((distanceSumGraph,1)) { case ((distanceSumGraph,iteration), (vertexIds, index)) => {
      logger.info(s"Closeness iteration ${iteration} from  ${numberOfIterations}")
      if(iteration % checkpointingFrequency==0){
        logger.info(s"Chceckpointing graph")
        distanceSumGraph.checkpoint()
        distanceSumGraph.vertices.foreachPartition((_)=>{})
        distanceSumGraph.edges.foreachPartition((_)=>{})
      }
      val shortestPaths = ShortestPathsAlgorithm.computeShortestPathsLengths(graph, InArrayPredicate(vertexIds), treatAsUndirected = vertexMeasureConfiguration.treatAsUndirected)
      val newValues=shortestPaths.vertices.map{
        case (vId,paths)=>(vId,paths.values().asScala.map(_.toDouble).map{
          case 0d => (0l, 0d)
          case other => (1l, pathMappingFunction(other))
        }.foldLeft((0l, 0d)) {
          case ((c1, v1), (c2, v2)) => (c1 + c2, v1 + v2)
        })
      }
      val out = distanceSumGraph.outerJoinVertices(newValues)((vId, oldValue, newValue) => {
        (oldValue, newValue) match {
          case ((oldPathsCount, oldPathsSum), Some((newPathsCount, newPathsSum))) => {
            (oldPathsCount + newPathsCount, oldPathsSum + newPathsSum)
          }
          case (_, None) => oldValue
        }
      })
      (out,iteration+1)
    }
    }._1.mapVertices {
      case (vId, (count, sum)) => closenessFunction(count, sum, normalize)
    }
    graph.unpersist(false)
    out

  }

  /**
    * Computes harmonic closeness centrality
    *
    * @param graph                      - computation graph
    * @param vertexMeasureConfiguration - configuration of computation
    * @param num                        - numeric for @ED
    * @tparam VD - vertex data type
    * @tparam ED - edge data type
    * @return graph where each vertex is associated with its harmonic closeness centrality
    */
  def computeHarmonic[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED])(implicit num: Numeric[ED]) = {
    computeUsing(graph,
      harmonicCloseness(graph.numVertices) _,
      harmonicClosenessValueMapper,
      vertexMeasureConfiguration)
  }

  /**
    * Computes standard closeness centrality
    *
    * @param graph                      - computation graph
    * @param vertexMeasureConfiguration - configuration of computation
    * @param num                        - numeric for @ED
    * @tparam VD - vertex data type
    * @tparam ED - edge data type
    * @return graph where each vertex is associated with its standard closeness centrality
    */
  override def compute[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED])(implicit num: Numeric[ED]): Graph[Double, ED] =
    computeUsing(graph,
      standardCloseness(graph.numVertices) _,
      standardClosenessValueMapper,
      vertexMeasureConfiguration)
}

package ml.sparkling.graph.operators.algorithms.aproximation

import java.util
import java.util.function.BiConsumer

import ml.sparkling.graph.api.operators.IterativeComputation.{VertexPredicate, _}
import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.Component
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes.{JDouble, JLong, JMap}
import ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation.{SimpleLPCoarsening}
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.{PathProcessor, SingleVertexProcessor}
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.{FastUtilWithDistance, FastUtilWithPath}
import ml.sparkling.graph.operators.predicates.{AllPathPredicate, ByIdPredicate, ByIdsPredicate}
import org.apache.spark.graphx.{EdgeTriplet, Graph, _}
import ml.sparkling.graph.operators.algorithms.shortestpaths.ShortestPathsAlgorithm
import ml.sparkling.graph.operators.algorithms.shortestpaths.pathprocessors.fastutils.FastUtilWithDistance.DataMap
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
 * Created by  Roman Bartusiak <riomus@gmail.com> on 07.02.17.
 */
case object ApproximatedShortestPathsAlgorithm {
  val logger = Logger.getLogger(ApproximatedShortestPathsAlgorithm.getClass())

  type PathModifier = (VertexId, VertexId, JDouble) => JDouble

  val defaultNewPath: (JDouble => JDouble) = (path: JDouble) => 3 * path + 2;
  val defaultPathModifier: PathModifier = (fromVertex: VertexId, toVertex: VertexId, path: JDouble) => defaultNewPath(path)

  def computeShortestPathsLengthsUsing[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPredicate: SimpleVertexPredicate = AllPathPredicate, treatAsUndirected: Boolean = true, modifier: PathModifier = defaultPathModifier)(implicit num: Numeric[ED]): Graph[Iterable[(VertexId, JDouble)], ED] = {
    val coarsedGraph = SimpleLPCoarsening.coarse(graph, treatAsUndirected)
    computeShortestPathsLengthsWithoutCoarsingUsing(graph, coarsedGraph, vertexPredicate, treatAsUndirected, modifier)
  }

  def computeShortestPathsLengthsWithoutCoarsingUsing[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], coarsedGraph: Graph[Component, ED], vertexPredicate: SimpleVertexPredicate = AllPathPredicate, treatAsUndirected: Boolean = true, modifier: PathModifier = defaultPathModifier)(implicit num: Numeric[ED]): Graph[Iterable[(VertexId, JDouble)], ED] = {
    val newVertexPredicate: VertexPredicate[Component] = AnyMatchingComponentPredicate(vertexPredicate);
    val coarsedShortestPaths: Graph[DataMap, ED] = ShortestPathsAlgorithm.computeShortestPathsLengths(coarsedGraph, newVertexPredicate, treatAsUndirected)
    aproximatePaths(graph, coarsedGraph, coarsedShortestPaths, modifier, vertexPredicate, treatAsUndirected)
  }

  def computeShortestPathsForDirectoryComputationUsing[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], coarsedGraph: Graph[Component, ED], vertexPredicate: SimpleVertexPredicate = AllPathPredicate, treatAsUndirected: Boolean = true, modifier: PathModifier = defaultPathModifier)(implicit num: Numeric[ED]): Graph[Iterable[(VertexId, JDouble)], ED] = {
    val newVertexPredicate: VertexPredicate[Component] = SimpleWrapper(vertexPredicate)
    val newIds: Set[VertexId] = coarsedGraph.vertices.filter {
      case (vid, component) => vertexPredicate(vid)
    }.treeAggregate[Set[VertexId]](Set())(seqOp = (agg, id) => agg ++ id._2, combOp = (agg1, agg2) => agg1 ++ agg2)
    val coarsedShortestPaths: Graph[DataMap, ED] = ShortestPathsAlgorithm.computeShortestPathsLengths(coarsedGraph, newVertexPredicate, treatAsUndirected)
    aproximatePaths(graph, coarsedGraph, coarsedShortestPaths, modifier, vertexPredicate = ByIdsPredicate(newIds), treatAsUndirected = treatAsUndirected)
  }

  def aproximatePaths[ED: ClassTag, VD: ClassTag](graph: Graph[VD, ED], coarsedGraph: Graph[Component, ED], coarsedShortestPaths: Graph[DataMap, ED], modifier: PathModifier = defaultPathModifier, vertexPredicate: SimpleVertexPredicate = AllPathPredicate, treatAsUndirected: Boolean)(implicit num: Numeric[ED]): Graph[Iterable[(VertexId, JDouble)], ED] = {
    logger.info("Aproximating shortes paths");
    logger.info(s"Number of partitions in coarsed graph ${coarsedShortestPaths.vertices.partitions.length}")
    val modifiedPaths = coarsedShortestPaths.vertices.mapPartitions(iter => iter.map {
      case (vertexId: VertexId, paths: DataMap) => {
        paths.forEach(new BiConsumer[JLong, JDouble] {
          override def accept(t: JLong, u: JDouble): Unit = {
            paths.put(t, modifier(vertexId, t, u))
          }
        });
        paths.remove(vertexId)
        (vertexId, paths)
      }
    })
    val fromMapped: RDD[(VertexId, (List[VertexId], JDouble))] = modifiedPaths
      .join(coarsedGraph.vertices, Math.max(modifiedPaths.partitions.length, coarsedGraph.vertices.partitions.length))
      .mapPartitions(
        iter => iter.flatMap {
          case (_, (data, componentFrom)) => {
            data.map {
              case (to, len) => (to.toLong, (componentFrom, len))
            }
          }
        }
      )

    logger.info(s"Number of partitions in fromMapped RDD ${fromMapped.partitions.length}")
    val toJoined: RDD[(VertexId, ((List[VertexId], JDouble), List[VertexId]))] = fromMapped
      .join(coarsedGraph.vertices, Math.max(fromMapped.partitions.length, coarsedGraph.vertices.partitions.length))
    val toMapped: RDD[(VertexId, (List[VertexId], JDouble))] = toJoined
      .mapPartitions((iter) => {
        iter.flatMap {
          case (_, ((componentFrom, len), componentTo)) => {
            componentFrom.map(
              (fromId) => (fromId, (componentTo, len))
            )
          }
        }
      })
    logger.info(s"Number of partitions in toMapped RDD ${toMapped.partitions.length}")
    val toMappedGroups: RDD[(VertexId, ListBuffer[(VertexId, JDouble)])] = toMapped.aggregateByKey(ListBuffer[(List[VertexId], JDouble)]())(
      (agg, data) => {
        agg += data; agg
      },
      (agg1, agg2) => {
        agg1 ++= agg2; agg1
      }
    ).mapPartitions((iter: Iterator[(VertexId, ListBuffer[(List[VertexId], JDouble)])]) => {
      iter.map {
        case (from, data) => (from, data.flatMap {
          case (datas, len) => datas.map((id) => (id, len))
        })
      }
    })
    val outGraph = Graph(toMappedGroups, graph.edges, ListBuffer[(VertexId, JDouble)]())
    val one: JDouble = 1.0
    val two: JDouble = 2.0
    val neighboursExchanged: RDD[(VertexId, ListBuffer[VertexId])] = outGraph.edges
      .mapPartitions((data) => {
        data.flatMap((edge) => {
          val toSrc = if (vertexPredicate(edge.dstId)) Iterable((edge.srcId, edge.dstId)) else Iterable.empty
          val toDst = if (vertexPredicate(edge.srcId) && treatAsUndirected) Iterable((edge.dstId, edge.srcId)) else Iterable.empty
          toSrc ++ toDst
        })
      })
      .aggregateByKey[ListBuffer[VertexId]](ListBuffer[VertexId]())((agg, e) => {
        agg += e; agg
      }, (agg1, agg2) => {
        agg1 ++= agg2; agg1
      })
    val graphWithNeighbours = outGraph.outerJoinVertices(neighboursExchanged) {
      case (_, _, Some(newData)) => newData
      case (_, _, None) => ListBuffer[VertexId]()
    }
    val secondLevelNeighbours: RDD[(VertexId, ListBuffer[VertexId])] = graphWithNeighbours.triplets.mapPartitions(
      (data) => {
        data.flatMap((edge) => {
          val toSrc = Iterable((edge.srcId, edge.dstAttr))
          val toDst = if (treatAsUndirected) Iterable((edge.dstId, edge.srcAttr)) else Iterable.empty
          toSrc ++ toDst
        })
      }
    ).aggregateByKey[ListBuffer[VertexId]](ListBuffer[VertexId]())((agg, e) => {
      agg ++= e; agg
    }, (agg1, agg2) => {
      agg1 ++= agg2; agg1
    })


    val neighbours = neighboursExchanged
      .fullOuterJoin(secondLevelNeighbours, coarsedGraph.vertices.partitions.length)
      .map {
        case (vId, (firstOpt, secondOpt)) => (vId, (firstOpt.map(d => d.map(id => (id, one))) :: (secondOpt.map(_.map(id => (id, two)))) :: Nil).flatten.flatten.filter(_._1 != vId))
      }

    val out: Graph[ListBuffer[(VertexId, JDouble)], ED] = outGraph.joinVertices(neighbours) {
      case (_, data, newData) => data ++ newData
    }
    out.mapVertices {
      case (id, data) =>
        val out = data.groupBy(_._1).mapValues(l => l.map(_._2).min).map(identity)
        if (vertexPredicate(id)) {
          out + (id -> 0.0)
        }
        else {
          out
        }
    }
  }

  def computeSingleShortestPathsLengths[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexId: VertexId, treatAsUndirected: Boolean = true, modifier: PathModifier = defaultPathModifier)(implicit num: Numeric[ED]): Graph[Iterable[(VertexId, JDouble)], ED] = {
    computeShortestPathsLengthsUsing(graph, ByIdPredicate(vertexId), treatAsUndirected, modifier = defaultPathModifier)
  }

  def computeShortestPaths[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPredicate: SimpleVertexPredicate = AllPathPredicate, treatAsUndirected: Boolean = true, modifier: PathModifier = defaultPathModifier)(implicit num: Numeric[ED]) = {
    computeShortestPathsLengthsUsing(graph, vertexPredicate, treatAsUndirected, modifier = defaultPathModifier)
  }

  def computeShortestPathsLengthsIterativeUsing[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                                            coarsedGraph: Graph[Component, ED],
                                                                            bucketSizeProvider: BucketSizeProvider[Component, ED],
                                                                            treatAsUndirected: Boolean = true,
                                                                            modifier: PathModifier = defaultPathModifier,
                                                                            checkpointingFrequency: Int = 20)(implicit num: Numeric[ED]): Graph[Iterable[(VertexId, JDouble)], ED] = {
    val coarsedShortestPaths: Graph[DataMap, ED] = ShortestPathsAlgorithm.computeShortestPathsLengthsIterative[Component, ED](coarsedGraph, bucketSizeProvider, treatAsUndirected,
      checkpointingFrequency = checkpointingFrequency)
    aproximatePaths(graph, coarsedGraph, coarsedShortestPaths, modifier, treatAsUndirected = treatAsUndirected)
  }

  def computeShortestPathsLengthsIterative[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], bucketSizeProvider: BucketSizeProvider[Component, ED],
                                                                       treatAsUndirected: Boolean = true,
                                                                       modifier: PathModifier = defaultPathModifier,
                                                                       checkpointingFrequency: Int = 20)(implicit num: Numeric[ED]): Graph[Iterable[(VertexId, JDouble)], ED] = {
    val coarsedGraph = SimpleLPCoarsening.coarse(graph, treatAsUndirected)
    computeShortestPathsLengthsIterativeUsing(graph, coarsedGraph, bucketSizeProvider, treatAsUndirected, checkpointingFrequency = checkpointingFrequency)
  }

  def computeAPSPToDirectory[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], outDirectory: String, treatAsUndirected: Boolean, bucketSize: Long)(implicit num: Numeric[ED]): Unit = {
    val coarsedGraph = SimpleLPCoarsening.coarse(graph, treatAsUndirected)
    logger.info(s"Coarsed graph has size ${coarsedGraph.vertices.count()} in comparision to ${graph.vertices.count()}")
    val verticesGroups = coarsedGraph.vertices.map(_._1).sortBy(k => k).collect().grouped(bucketSize.toInt).zipWithIndex.toList
    val numberOfIterations = verticesGroups.length;
    (verticesGroups).foreach {
      case (group, iteration) => {
        logger.info(s"Approximated Shortest Paths iteration ${iteration + 1} from  ${numberOfIterations}")
        val shortestPaths = ApproximatedShortestPathsAlgorithm.computeShortestPathsForDirectoryComputationUsing(graph, coarsedGraph, new ByIdsPredicate(group.toSet), treatAsUndirected)
        val joinedGraph = graph
          .outerJoinVertices(shortestPaths.vertices)((vId, data, newData) => (data, newData.getOrElse(Iterable())))
        joinedGraph.vertices.values.map {
          case (vertex, data) => {
            val dataStr = data
              .map {
                case (key, value) => s"${key}:${value}"
              }.mkString(";")
            s"$vertex;$dataStr"
          }
        }.saveAsTextFile(s"${outDirectory}/from_${group.head}")
        shortestPaths.unpersist(blocking = false)
      }
    }

    graph.vertices.map(t => List(t._1, t._2).mkString(";")).saveAsTextFile(s"${outDirectory}/index")
  }

}

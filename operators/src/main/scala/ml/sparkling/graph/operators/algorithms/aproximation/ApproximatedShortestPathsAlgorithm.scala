package ml.sparkling.graph.operators.algorithms.aproximation

import java.util
import java.util.function.BiConsumer

import ml.sparkling.graph.api.operators.IterativeComputation.{VertexPredicate, _}
import ml.sparkling.graph.api.operators.algorithms.coarsening.CoarseningAlgorithm.Component
import ml.sparkling.graph.api.operators.algorithms.shortestpaths.ShortestPathsTypes.{JDouble, JLong, JMap}
import ml.sparkling.graph.operators.algorithms.coarsening.labelpropagation.LPCoarsening
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
case object ApproximatedShortestPathsAlgorithm  {
  val logger=Logger.getLogger(ApproximatedShortestPathsAlgorithm.getClass())

  type PathModifier=(VertexId,VertexId,JDouble)=>JDouble

  val defaultNewPath:(JDouble=>JDouble)= (path:JDouble)=>3*path+2;
  val defaultPathModifier:PathModifier= (fromVertex:VertexId, toVertex:VertexId, path:JDouble)=>defaultNewPath(path)

  def computeShortestPathsLengthsUsing[VD:ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPredicate: SimpleVertexPredicate= AllPathPredicate, treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[Iterable[(VertexId,JDouble)],ED] = {
    val coarsedGraph=LPCoarsening.coarse(graph,treatAsUndirected)
    computeShortestPathsLengthsWithoutCoarsingUsing(graph,coarsedGraph,vertexPredicate,treatAsUndirected,modifier)
  }

  def computeShortestPathsLengthsWithoutCoarsingUsing[VD:ClassTag, ED: ClassTag](graph: Graph[VD, ED], coarsedGraph: Graph[Component, ED], vertexPredicate: SimpleVertexPredicate= AllPathPredicate, treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[Iterable[(VertexId,JDouble)],ED] = {
    val newVertexPredicate:VertexPredicate[Component]=AnyMatchingComponentPredicate(vertexPredicate);
    val coarsedShortestPaths: Graph[DataMap, ED] =ShortestPathsAlgorithm.computeShortestPathsLengths(coarsedGraph,newVertexPredicate,treatAsUndirected)
    aproximatePaths(graph, coarsedGraph, coarsedShortestPaths,modifier,vertexPredicate,treatAsUndirected)
  }
  def computeShortestPathsLengthsWithoutCoarsingUsing2[VD:ClassTag, ED: ClassTag](graph: Graph[VD, ED], coarsedGraph: Graph[Component, ED], vertexPredicate: SimpleVertexPredicate= AllPathPredicate, treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[Iterable[(VertexId,JDouble)],ED] = {
    val newVertexPredicate:VertexPredicate[Component]=SimpleWrapper(vertexPredicate)
    val newIds: Set[VertexId] =coarsedGraph.vertices.filter{
      case (vid, component)=>vertexPredicate(vid)
    }.treeAggregate[Set[VertexId]](Set())(seqOp=(agg, id)=>agg++id._2,combOp= (agg1, agg2)=>agg1++agg2)
    val coarsedShortestPaths: Graph[DataMap, ED] =ShortestPathsAlgorithm.computeShortestPathsLengths(coarsedGraph,newVertexPredicate,treatAsUndirected)
    aproximatePaths(graph, coarsedGraph, coarsedShortestPaths,modifier,vertexPredicate=ByIdsPredicate(newIds),treatAsUndirected=treatAsUndirected)
  }

  def aproximatePaths[ED: ClassTag, VD:ClassTag](graph: Graph[VD, ED], coarsedGraph: Graph[Component, ED], coarsedShortestPaths: Graph[DataMap, ED], modifier:PathModifier=defaultPathModifier, vertexPredicate: SimpleVertexPredicate= AllPathPredicate, treatAsUndirected:Boolean)(implicit num:Numeric[ED]):Graph[Iterable[(VertexId,JDouble)],ED] = {
    logger.info("Aproximating shortes paths");
    val modifiedPaths = coarsedShortestPaths.vertices.mapPartitions(iter=>iter.map{
      case (vertexId: VertexId, paths: DataMap) => {
        paths.forEach(new BiConsumer[JLong,JDouble] {
          override def accept(t: JLong, u: JDouble): Unit = {
            paths.put(t,modifier(vertexId,t,u))
          }
        });
        (vertexId,paths)
      }
    }) .cache()
    val fromMapped: RDD[(VertexId, (List[VertexId], JDouble))] =modifiedPaths.join(coarsedGraph.vertices).mapPartitions(
      iter=>iter.flatMap{
        case (from,(data,componentFrom) )=>{
          data.map{
            case (to,len)=>(to.toLong,(componentFrom,len))
          }
        }
      }
    ).cache()

    val toJoined: RDD[(VertexId, ((List[VertexId], JDouble), List[VertexId]))] =fromMapped.join(coarsedGraph.vertices).cache()
    val toMapped: RDD[(VertexId, (List[VertexId], JDouble))] =toJoined.mapPartitions((iter)=>{
      iter.flatMap{
        case (to,((componentFrom,len),componentTo))=>{
          componentFrom.map(
            (fromId)=>(fromId,(componentTo,len))
          )
        }
      }
    })
    val toMappedGroups=toMapped.aggregateByKey(ListBuffer[(List[VertexId], JDouble)]())(
      (agg,data)=>{agg+=data;agg},
      (agg1,agg2)=>{agg1++=agg2;agg1}
    ).mapPartitions((iter: Iterator[(VertexId, ListBuffer[(List[VertexId], JDouble)])]) =>{
      iter.map{
        case (from,data)=>(from,data.flatMap{
          case (datas,len)=>datas.map((id)=>(id,len))
        })
      }
    })
    val outGraph=Graph(toMappedGroups, graph.edges,ListBuffer[(VertexId, JDouble)]()).cache()
    val one:JDouble=1.0

    val out=outGraph.mapVertices{case (vid,data)=>data}.pregel[Iterable[(VertexId,JDouble)]](Iterable(),maxIterations = 1)(
    vprog=(vid,data,msg)=>{
    val out=data++msg.filter(_._1!=vid).groupBy{
      case (id,len)=>id
    }.mapValues(_.map(_._2).min)
      out
    },
    sendMsg=(triplet)=> {
      val extSrc=if(vertexPredicate(triplet.dstId)) Iterable((triplet.dstId,one)) else Iterable()
      val extDst=if(vertexPredicate(triplet.srcId)) Iterable((triplet.srcId,one)) else Iterable()
      val toSrc= Iterator((triplet.srcId,(extSrc++triplet.dstAttr.filter(_._1!=triplet.srcId).map{case (id,len)=>(id,(len+one).asInstanceOf[JDouble])})))
      val toDst=if(treatAsUndirected) Iterator((triplet.dstId,(extDst++triplet.srcAttr.filter(_._1!=triplet.dstId).map{case (id,len)=>(id,(len+one).asInstanceOf[JDouble])}))) else Iterator()
        toSrc++toDst
      },
    mergeMsg=(a,b)=>a++b
    )
    out.mapVertices{case (vid,data)=>data.groupBy{
      case (id,len)=>id
    }.mapValues(_.map(_._2).min).map(identity)
    }
  }

  def computeSingleShortestPathsLengths[VD:ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexId: VertexId, treatAsUndirected: Boolean = false, modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[Iterable[(VertexId,JDouble)],ED]= {
    computeShortestPathsLengthsUsing(graph,ByIdPredicate(vertexId),treatAsUndirected,modifier=defaultPathModifier)
  }

  def computeShortestPaths[VD:ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPredicate: SimpleVertexPredicate = AllPathPredicate, treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]) = {
    computeShortestPathsLengthsUsing(graph,vertexPredicate,treatAsUndirected,modifier=defaultPathModifier)
  }

  def computeShortestPathsLengthsIterativeUsing[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], coarsedGraph: Graph[Component, ED], bucketSizeProvider: BucketSizeProvider[Component,ED], treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[Iterable[(VertexId,JDouble)],ED] = {
    val coarsedShortestPaths: Graph[DataMap, ED] =ShortestPathsAlgorithm.computeShortestPathsLengthsIterative[Component,ED](coarsedGraph,bucketSizeProvider,treatAsUndirected)
    aproximatePaths(graph, coarsedGraph, coarsedShortestPaths,modifier,treatAsUndirected=treatAsUndirected)
  }

  def computeShortestPathsLengthsIterative[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], bucketSizeProvider: BucketSizeProvider[Component,ED], treatAsUndirected: Boolean = false,modifier:PathModifier=defaultPathModifier)(implicit num: Numeric[ED]):Graph[Iterable[(VertexId,JDouble)],ED] = {
    val coarsedGraph=LPCoarsening.coarse(graph,treatAsUndirected)
    computeShortestPathsLengthsIterativeUsing(graph,coarsedGraph,bucketSizeProvider,treatAsUndirected)
  }

  def computeAPSPToDirectory[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], outDirectory: String, treatAsUndirected: Boolean, bucketSize:Long)(implicit num: Numeric[ED]): Unit = {
    val coarsedGraph=LPCoarsening.coarse(graph,treatAsUndirected)
    logger.info(s"Coarsed graph has size ${coarsedGraph.vertices.count()} in comparision to ${graph.vertices.count()}")
    val verticesGroups = coarsedGraph.vertices.map(_._1).sortBy(k => k).collect().grouped(bucketSize.toInt).zipWithIndex.toList
    val numberOfIterations=verticesGroups.length;
    graph.cache()
    coarsedGraph.cache()
    (verticesGroups).foreach{
      case (group,iteration) => {
        logger.info(s"Approximated Shortest Paths iteration ${iteration+1} from  ${numberOfIterations}")
        val shortestPaths = ApproximatedShortestPathsAlgorithm.computeShortestPathsLengthsWithoutCoarsingUsing2(graph,coarsedGraph, new ByIdsPredicate(group.toSet), treatAsUndirected)
        val joinedGraph = graph
          .outerJoinVertices(shortestPaths.vertices)((vId, data, newData) => (data, newData.getOrElse(Iterable())))
        joinedGraph.vertices.values.map {
          case (vertex, data) => {
            val dataStr = data
              .map{
                case (key,value)=>s"${key}:${value}"
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

package ml.sparkling.graph.operators.measures.clustering

import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import ml.sparkling.graph.api.operators.measures.{VertexMeasure, VertexMeasureConfiguration}
import ml.sparkling.graph.operators.measures.utils.CollectionsUtils._
import ml.sparkling.graph.operators.measures.utils.{CollectionsUtils, NeighboursUtils}
import ml.sparkling.graph.operators.predicates.AllPathPredicate
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Computes local clustering
 */
object LocalClustering extends VertexMeasure[Double] {
  /**
   * Computes local clustering
   * @param graph - computation graph
   * @param vertexMeasureConfiguration - configuration of computation
   * @param num - numeric for @ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its local clustering
   */
   override def compute[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                    vertexMeasureConfiguration: VertexMeasureConfiguration[VD,ED])
                                                   (implicit num: Numeric[ED]) = {
    val firstLevelNeighboursGraph = NeighboursUtils.getWithNeighbours(graph, vertexMeasureConfiguration.treatAsUndirected, AllPathPredicate)
    val localClusteringSums=firstLevelNeighboursGraph.aggregateMessages[Double](
      sendMsg=edgeContext=>{
      def messageCreator=(neighbours1:LongOpenHashSet,neighbours2:LongOpenHashSet)=>{
         intersectSize(neighbours1,neighbours2)
      }
      val message=messageCreator(edgeContext.srcAttr,edgeContext.dstAttr)
      edgeContext.sendToSrc(message)
      if(vertexMeasureConfiguration.treatAsUndirected){
      edgeContext.sendToDst(message)
      }
    },
    mergeMsg=(a,b)=>a+b)
    firstLevelNeighboursGraph.outerJoinVertices(localClusteringSums)((vId,oldValue,newValue)=>(newValue.getOrElse(0d),oldValue)).mapVertices {
      case (vId, (sum, neighbours)) => {
        val possibleConnections = neighbours.size * (neighbours.size - 1)
        if (possibleConnections == 0) 0d else sum / possibleConnections
      }
    }
  }
}

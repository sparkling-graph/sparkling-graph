package sparkling.graph.operators.measures

import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongOpenHashSet}
import org.apache.spark.graphx.{Graph}
import sparkling.graph.api.operators.measures.{VertexMeasureConfiguration, VertexMeasure}
import sparkling.graph.operators.measures.utils.NeighboursUtils
import sparkling.graph.operators.measures.utils.NeighboursUtils.NeighbourSet
import sparkling.graph.operators.predicates.{ByIdsPredicate, AllPathPredicate}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import sparkling.graph.operators.measures.utils.CollectionsUtils._
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object VertexEmbeddedness extends VertexMeasure[Double] {
  /**
   * Generic method for embeddedness that should be used for extensions. Computations are done using super-step approach
   * @param graph - computation graph
   * @param vertexMeasureConfiguration - configuration of computation
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its  embeddedness
   */
  def computeEmbeddedness[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                      vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED]) = {
    val firstLevelNeighboursGraph = NeighboursUtils.getWithNeighbours(graph, vertexMeasureConfiguration.treatAsUndirected, AllPathPredicate)
    val embeddednessSums=firstLevelNeighboursGraph.aggregateMessages[Double](
      sendMsg=edgeContext=>{
      def messageCreator=(neighbours1:LongOpenHashSet,neighbours2:LongOpenHashSet)=>{
      val sizeOfIntersection=intersectSize(neighbours1,neighbours2)
         val denominator = neighbours1.size()+neighbours2.size()-sizeOfIntersection
            val numerator = sizeOfIntersection.toDouble
            if (denominator == 0) 0. else numerator / denominator
      }
      val message=messageCreator(edgeContext.srcAttr,edgeContext.dstAttr)
      edgeContext.sendToSrc(message)
      if(vertexMeasureConfiguration.treatAsUndirected){
      edgeContext.sendToDst(message)
      }

    },
    mergeMsg=(a,b)=>a+b)
    firstLevelNeighboursGraph.outerJoinVertices(embeddednessSums)((vId,oldValue,newValue)=>(newValue.getOrElse(0d),oldValue)).mapVertices { case (vId, (numerator, neighbours)) => {
      val myNeghboursSize = neighbours.size()
      if (myNeghboursSize == 0) 0. else numerator / neighbours.size()
    }
    }

  }

  /**
   * Computes  embeddedness  of each vertex
   * @param graph - computation graph
   * @param vertexMeasureConfiguration - configuration of computation
   * @param num - numeric for @ED
   * @tparam VD - vertex data type
   * @tparam ED - edge data type
   * @return graph where each vertex is associated with its  embeddedness
   */
  override def compute[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                   vertexMeasureConfiguration: VertexMeasureConfiguration[VD, ED])(implicit num: Numeric[ED]) = computeEmbeddedness(graph, vertexMeasureConfiguration)



}

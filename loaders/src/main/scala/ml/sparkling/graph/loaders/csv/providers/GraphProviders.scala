package ml.sparkling.graph.loaders.csv.providers

import ml.sparkling.graph.loaders.csv.types.Types
import ml.sparkling.graph.loaders.csv.types.Types.ToVertexId
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object GraphProviders {
  val defaultStorageLevel=StorageLevel.MEMORY_ONLY
  def simpleGraphBuilder[VD: ClassTag, ED: ClassTag](defaultVertex: Option[VD]=None,
                                                     vertexProvider: Row => Seq[(VertexId, VD)],
                                                     edgeProvider: Row => Seq[Edge[ED]],
                                                     edgeStorageLevel: StorageLevel = defaultStorageLevel,
                                                     vertexStorageLevel: StorageLevel =defaultStorageLevel)
                                                    (dataFrame: DataFrame): Graph[VD, ED] = {

    def mapRows[MT: ClassTag](mappingFunction: (Row) => Seq[MT]): RDD[MT] = {
      dataFrame.rdd.mapPartitionsWithIndex((id, rowIterator) => {
        rowIterator.flatMap { case row => mappingFunction(row) }
      })
    }

    val vertices: RDD[(VertexId, VD)] = mapRows(vertexProvider)
    val edges: RDD[Edge[ED]] = mapRows(edgeProvider)
    defaultVertex match{
      case None => Graph(vertices,edges,edgeStorageLevel=edgeStorageLevel,vertexStorageLevel=vertexStorageLevel)
      case Some(defaultVertexValue)=> Graph(vertices,edges,defaultVertexValue,edgeStorageLevel,vertexStorageLevel)
    }

  }

  def indexedGraphBuilder[VD:ClassTag, ED: ClassTag](defaultVertex: Option[VD]=None,
                                                      vertexProvider: (Row, ToVertexId[VD]) => Seq[(VertexId, VD)],
                                                      edgeProvider: (Row, ToVertexId[VD]) => Seq[Edge[ED]],
                                                      columnsToIndex: Seq[Int],
                                                      edgeStorageLevel: StorageLevel = defaultStorageLevel,
                                                      vertexStorageLevel: StorageLevel = defaultStorageLevel)
                                                     (dataFrame: DataFrame): Graph[VD, ED] = {
    val index = dataFrame.rdd.flatMap(row => columnsToIndex.map(row(_))).distinct().zipWithUniqueId().collect().toMap
    def extractIdFromIndex(vertex: VD) = index(vertex)
    simpleGraphBuilder(defaultVertex,
      vertexProvider(_: Row, extractIdFromIndex _),
      edgeProvider(_: Row, extractIdFromIndex _),
      edgeStorageLevel,
      vertexStorageLevel)(dataFrame)

  }
}
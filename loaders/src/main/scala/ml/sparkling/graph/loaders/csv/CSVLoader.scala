package ml.sparkling.graph.loaders.csv


import ml.sparkling.graph.api.loaders.{CSVTypes, Types}
import ml.sparkling.graph.loaders.csv.providers.{VertexProviders, EdgeProviders, GraphProviders}
import ml.sparkling.graph.loaders.csv.utils.DefaultTransformers
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.sql.{Row, SQLContext}
import CSVTypes.{EdgeAttributeExtractor, GraphBuilder}
import Types.ToVertexId
import DataFrameReaderConfigurator.addAbilityToConfigureDataFrameReader
import GraphProviders.{indexedGraphBuilder, simpleGraphBuilder}
import DefaultTransformers._

import scala.reflect.ClassTag

/**
 * Main class of csv loader
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object CSVLoader {
  private val dataBricksFormat: String = "com.databricks.spark.csv"

  /**
   *  Main method for graph loading from csv, should be used for extension and further development,
   *  please use case specific loading methods present in that object
   * @param file - CSV file
   * @param graphBuilder - graph builder to handle graph creation from csv file
   * @param csvLoaderConfig - csv loader configuration
   * @param sc - spark context
   * @tparam VD - vertex attribute type
   * @tparam ED - edge attribute type
   * @return Graph[VD,ED] - loaded graph
   */
  def loadGraphFromCSV[VD, ED]
  (file: String,
   graphBuilder: GraphBuilder[VD, ED],
   csvLoaderConfig: CsvLoaderConfig,
    partitions:Int
    )
  (implicit sc: SparkContext): Graph[VD, ED] = {
    val sqlContext = new SQLContext(sc)
    val dataFrame = sqlContext.read
      .format(dataBricksFormat)
      .applyConfiguration(csvLoaderConfig)
      .load(file)

    graphBuilder(if(partitions!=0) dataFrame.repartition(partitions) else dataFrame)
  }

  /**
   * Simple load graph from CSV. Columns from witch vertices will be created must be numeric identifiers of vertices
   * @param file - CSV input file
   * @param csvLoaderConfig - Loader config, with default value
   * @param column1 - first column to be selected as vertex, default:0
   * @param column2 - second column to be selected as vertex, default:1
   * @param defaultVertex - default vertex, has default value
   * @param sc - spark contex
   * @return Graph[String,Double] - loaded graph
   */
  def loadGraphFromCSV(file: String,
                       csvLoaderConfig: CsvLoaderConfig = CsvLoaderConfig(),
                       column1: Int = 0,
                       column2: Int = 1,
                        defaultVertex:String="<EMPTY VERTEX>",
                       partitions:Int=0)
                      (implicit sc: SparkContext): Graph[String, Double] = {
    loadGraphFromCSV(file, simpleGraphBuilder(defaultVertex = defaultVertex,
      vertexProvider=VertexProviders.columnsAsVertex[String](Seq(column1,column2),_:Row),
      edgeProvider=EdgeProviders.twoColumnsMakesEdge[String](column1,column2,_:Row)) _, csvLoaderConfig,partitions)
  }

  /**
   * Load graph from given CSV file, indexing vertices toprovide each unique ID.
   * @param file - input CSV file
   * @param csvLoaderConfig - CSV loader config , has default value
   * @param defaultVertex - default Vertex value
   * @param column1 - first column to be selected as vertex, default:0
   * @param column2 - second column to be selected as vertex, default:1
   * @param edgeAttributeProvider - provider for edge attribute, default 1L
   * @param sc - spark context
   * @tparam VD - vertex type
   * @tparam ED - edge type
   * @return Graph[VD,ED] - loaded graph
   */

  def loadGraphFromCSVWitVertexIndexing[VD: ClassTag, ED: ClassTag](file: String,
                                                                    csvLoaderConfig: CsvLoaderConfig = CsvLoaderConfig(),
                                                                    defaultVertex: VD = null,
                                                                    column1: Int = 0,
                                                                    column2: Int = 1,
                                                                    edgeAttributeProvider: EdgeAttributeExtractor[ED] = defaultEdgeAttribute _,
                                                                     partitions:Int=0)
                                                                   (implicit sc: SparkContext): Graph[VD, ED] = {
    loadGraphFromCSV(file,
      indexedGraphBuilder(defaultVertex = defaultVertex,
        vertexProvider=VertexProviders.columnsAsVertex[VD](Seq(column1,column2),_:Row,_:ToVertexId[VD]),
        edgeProvider=EdgeProviders.twoColumnsMakesEdge[VD,ED](column1,column2, _:Row,_:ToVertexId[VD],edgeAttributeProvider = edgeAttributeProvider),
        columnsToIndex=Seq(column1, column2)),
      csvLoaderConfig,
      partitions
    )
  }


}

package ml.sparkling.graph.loaders.csv

import java.net.URL

import ml.sparkling.graph.api.loaders.GraphLoading._
import ml.sparkling.graph.loaders.csv.GraphFromCsv.LoaderParameters._
import ml.sparkling.graph.loaders.csv.providers.GraphProviders._
import ml.sparkling.graph.loaders.csv.providers.{EdgeProviders, VertexProviders}
import ml.sparkling.graph.loaders.csv.types.CSVTypes.GraphBuilder
import ml.sparkling.graph.loaders.csv.types.Types._
import ml.sparkling.graph.loaders.csv.utils.DefaultTransformers._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object GraphFromCsv {
  val log = Logger.getLogger(getClass.getName)

  type EdgeProvider[V,E]=(Int,Int,Row,ToVertexId[V])=>Seq[Edge[E]]
  type GraphBuilderCreator[V,E]=LoaderConfiguration[V,E]=>GraphBuilder[V,E]

  case class LoaderConfiguration[V:ClassTag,E:ClassTag](
                                                         var edgeProvider:EdgeProvider[V,E],
                                                         var loader:GraphBuilderCreator[V,E],
                                                         var defaultVertex:Option[V]=None,
                                                         var columnOne: Integer=0,
                                                         var columnTwo:Integer=1,
                                                         var partitions:Integer=0,
                                                         var csvLoaderConfig: CsvLoaderConfig=CsvLoaderConfig()
                                                         )

  object CSV extends FromPathLoader{
    def apply(path:String):GraphLoader=CSVGraphLoader(path)
    def apply(path:URL):GraphLoader=this(path.toString)
  }

  def standardGraphBuilderCreator[V:ClassTag,E:ClassTag](configuration:LoaderConfiguration[V,E]):GraphBuilder[V,E]={
    simpleGraphBuilder(
      defaultVertex = configuration.defaultVertex,
      vertexProvider=VertexProviders.columnsAsVertex[V](Seq(configuration.columnOne,configuration.columnTwo),_:Row),
      edgeProvider=configuration.edgeProvider(configuration.columnOne,configuration.columnTwo,_,numberToVertexId _))
  }

  def indexedGraphBuilderCreator[V:ClassTag,E:ClassTag](configuration:LoaderConfiguration[V,E]):GraphBuilder[V,E]={
    indexedGraphBuilder(defaultVertex = configuration.defaultVertex,
      vertexProvider=VertexProviders.columnsAsVertex[V](Seq(configuration.columnOne,configuration.columnTwo),_:Row,_:ToVertexId[V]),
      edgeProvider=configuration.edgeProvider(configuration.columnOne,configuration.columnTwo,_:Row,_:ToVertexId[V]),
      columnsToIndex=Seq(configuration.columnOne, configuration.columnTwo))
  }
  


  case class CSVGraphLoader(path:String) extends GraphLoader{
    override def load[VD: ClassTag, ED: ClassTag](parameters: List[Parameter])(implicit sc:SparkContext): Graph[VD, ED] = {
       val configuration:LoaderConfiguration[VD,ED]= parameters.foldLeft(getDefaultConfiguration[VD,ED]())(
          (conf:LoaderConfiguration[VD,ED],parameter:Parameter)=>{
            parameter match{
              case Delimiter(d)=>conf.csvLoaderConfig.delimiter=d;conf
              case Quotation(q)=>conf.csvLoaderConfig.quote=q;conf
              case FirstVertexColumn(c)=>conf.columnOne=c;conf
              case SecondVertexColumn(c)=>conf.columnTwo=c;conf
              case Partitions(p)=>conf.partitions=p;conf
              case DefaultVertex(v:VD)=>conf.defaultVertex=Option(v);conf
              case Schema(s)=>conf.csvLoaderConfig.schema=Option(s);conf
              case EdgeValue(value)=>conf.edgeProvider=valueEdge[VD,ED](value.asInstanceOf[ED]);conf;
              case EdgeColumn(column)=>conf.edgeProvider=columnEdge[VD,ED](column);conf;
              case NoHeader=>conf.csvLoaderConfig.header=false;conf
              case SchemaInference=>conf.csvLoaderConfig.inferSchema=true;conf
              case Indexing=>conf.loader=indexedGraphBuilderCreator[VD,ED] _;conf
              case parameter => log.error(s"Unnown parameter! ${parameter}"); conf
            }
          }
        )
      CSVLoader.loadGraphFromCSV(path,
        configuration.loader(configuration),
        configuration.csvLoaderConfig,
        configuration.partitions)
    }
  }

  object LoaderParameters {

    /**
     * Delimiter used to separate fields
     * @param value
     */
    case class Delimiter(override val value: String) extends WithValueParameter[String]

    /**
     * String used to quite fields
     * @param value
     */
    case class Quotation(override val value: String) extends WithValueParameter[String]

    /**
     * Id of column used as first vertex (base 0)
     * @param value
     */
    case class FirstVertexColumn(override val value: Integer) extends WithValueParameter[Integer]

    /**
     * Id of column used as second vertex (base 0)
     * @param value
     */
    case class SecondVertexColumn(override val value: Integer) extends WithValueParameter[Integer]
    
    /**
     * If you want each edge to have same value associated with, you can specify it by using that parameter
     * @param value
     */
    case class EdgeValue[EV](override val value: EV) extends WithValueParameter[EV]

    /**
     * Id of column used as edge value
     * @param value
     */
    case class EdgeColumn(override val value: Integer) extends WithValueParameter[Integer]

    /**
     * Number of partitions used for data loading
     * @param value
     */
    case class Partitions(override val value: Integer) extends WithValueParameter[Integer]

    /**
     * Default value of vertex (required by Spark GraphX)
     * @param value
     * @tparam V
     */
    case class DefaultVertex[V](override val value: V) extends WithValueParameter[V]

    /**
     * Schema of CSV file
     * @param value
     */
    case class Schema(override val value: StructType) extends WithValueParameter[StructType]

    /**
     * Indicates that CSV has no header
     */
    case object NoHeader extends Parameter

    /**
     * Indicates that automatic schema inference schould be done during loading process
     */
    case object SchemaInference extends Parameter


    /**
     * Indicates that vertex columns are not numerical and automatic vertexId asigment should be done
     */
    case object Indexing extends Parameter

  }

  def valueEdge[VD:ClassTag,ED:ClassTag](value:ED)={
    EdgeProviders.twoColumnsMakesEdge[VD,ED](_:Int,_:Int,_:Row, _:ToVertexId[VD],row=>value)
  }

  def columnEdge[VD:ClassTag,ED:ClassTag](column:Int)={
    EdgeProviders.twoColumnsMakesEdge[VD,ED](_:Int,_:Int,_:Row,_:ToVertexId[VD],row=>row.getAs[ED](column))
  }

  def rowToType[E:ClassTag](row:Row):E={
    val string=implicitly[ClassTag[String]]
    val integer=implicitly[ClassTag[Integer]]
    implicitly[ClassTag[E]] match{
      case `string`  => "".asInstanceOf[E]
      case ClassTag.Int => 1.asInstanceOf[E]
      case `integer` => 1.asInstanceOf[E]
      case ClassTag.Double => 1d.asInstanceOf[E]
      case ClassTag.Float => 1f.asInstanceOf[E]
      case typ=>log.debug(s"Unsuported edge type, please configure edge attribute provider ${typ}"); "".asInstanceOf[E]
    }
  }

  def vertexDefaultValue[V:ClassTag]():Option[V]={
    val string=implicitly[ClassTag[String]]
    val integer=implicitly[ClassTag[Integer]]
    implicitly[ClassTag[V]] match{
      case `string`  => Option("".asInstanceOf[V])
      case ClassTag.Int => Option(1.asInstanceOf[V])
      case `integer` => Option(1.asInstanceOf[V])
      case ClassTag.Double => Option(1d.asInstanceOf[V])
      case ClassTag.Float => Option(1f.asInstanceOf[V])
      case typ=>log.debug(s"Unsuported edge type, please configure edge attribute provider ${typ}"); Option("").asInstanceOf[Option[V]]
    }
  }

  def getDefaultConfiguration[VD:ClassTag,ED:ClassTag]():LoaderConfiguration[VD,ED]={
    val defaultEdgeProvider =  EdgeProviders.twoColumnsMakesEdge[VD,ED](_:Int,_:Int,_:Row,_:ToVertexId[VD],rowToType[ED] _)
    LoaderConfiguration[VD,ED](defaultEdgeProvider,standardGraphBuilderCreator[VD,ED] _,vertexDefaultValue[VD]())
  }
}

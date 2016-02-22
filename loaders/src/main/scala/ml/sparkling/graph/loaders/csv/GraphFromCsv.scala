package ml.sparkling.graph.loaders.csv

import ml.sparkling.graph.api.loaders.GraphLoading._
import ml.sparkling.graph.loaders.csv.GraphFromCsv.LoaderParameters._
import ml.sparkling.graph.loaders.csv.providers.GraphProviders._
import ml.sparkling.graph.loaders.csv.providers.{EdgeProviders, VertexProviders}
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

  object CSV extends FromPathLoader{
    def apply(path:String)=CSVGraphLoader(path)
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
              case DefaultVertex(v:VD)=>conf.defaultVertex=v;conf
              case Schema(s)=>conf.csvLoaderConfig.schema=s;conf
              case EdgeValue(value)=>conf.edgeProvider=valueEdge[VD,ED](value.asInstanceOf[ED]);conf;
              case EdgeColumn(column)=>conf.edgeProvider=columnEdge[VD,ED](column);conf;
              case NoHeader=>conf.csvLoaderConfig.header=false;conf
              case SchemaInference=>conf.csvLoaderConfig.inferSchema=true;conf
              case parameter => log.error(s"Unnown parameter! ${parameter}"); conf
            }
          }
        )
      CSVLoader.loadGraphFromCSV(path,
        simpleGraphBuilder(defaultVertex = configuration.defaultVertex,
          vertexProvider=VertexProviders.columnsAsVertex[VD](Seq(configuration.columnOne,configuration.columnTwo),_:Row),
          edgeProvider=configuration.edgeProvider(configuration.columnOne,configuration.columnTwo,_)) _,
        configuration.csvLoaderConfig,
        configuration.partitions)
    }
  }
  object LoaderParameters {
    type EdgeProvider[E]=(Int,Int,Row)=>Seq[Edge[E]]

    case class LoaderConfiguration[V:ClassTag,E:ClassTag](
                                                           var edgeProvider:EdgeProvider[E],
                                                           var defaultVertex:V,
                                                           var columnOne: Integer=0,
                                      var columnTwo:Integer=1,
                                      var partitions:Integer=0,
                                      var csvLoaderConfig: CsvLoaderConfig=CsvLoaderConfig()
                                       )

    case class Delimiter(override val value: String) extends WithValueParameter[String]

    case class Quotation(override val value: String) extends WithValueParameter[String]

    case class FirstVertexColumn(override val value: Integer) extends WithValueParameter[Integer]

    case class SecondVertexColumn(override val value: Integer) extends WithValueParameter[Integer]

    case class EdgeValue[EV](override val value: EV) extends WithValueParameter[EV]

    case class EdgeColumn(override val value: Integer) extends WithValueParameter[Integer]

    case class Partitions(override val value: Integer) extends WithValueParameter[Integer]

    case class DefaultVertex[V](override val value: V) extends WithValueParameter[V]

    case class Schema(override val value: StructType) extends WithValueParameter[StructType]

    case object NoHeader extends Parameter

    case object SchemaInference extends Parameter

  }

  def valueEdge[VD:ClassTag,ED:ClassTag](value:ED)={
    EdgeProviders.twoColumnsMakesEdge[VD,ED](_:Int,_:Int,_:Row,numberToVertexId _,row=>value)
  }

  def columnEdge[VD:ClassTag,ED:ClassTag](column:Int)={
    EdgeProviders.twoColumnsMakesEdge[VD,ED](_:Int,_:Int,_:Row,numberToVertexId _,row=>row.getAs[ED](column))
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
      case typ=>log.debug(s"Unsuported edge type, please configure edge attribute provider ${typ}"); null.asInstanceOf[E]
    }
  }

  def vertexDefaultValue[V:ClassTag]():V={
    val string=implicitly[ClassTag[String]]
    val integer=implicitly[ClassTag[Integer]]
    implicitly[ClassTag[V]] match{
      case `string`  => "".asInstanceOf[V]
      case ClassTag.Int => 1.asInstanceOf[V]
      case `integer` => 1.asInstanceOf[V]
      case ClassTag.Double => 1d.asInstanceOf[V]
      case ClassTag.Float => 1f.asInstanceOf[V]
      case typ=>log.debug(s"Unsuported edge type, please configure edge attribute provider ${typ}"); null.asInstanceOf[V]
    }
  }

  def getDefaultConfiguration[VD:ClassTag,ED:ClassTag]():LoaderConfiguration[VD,ED]={
    val defaultEdgeProvider=  EdgeProviders.twoColumnsMakesEdge[VD,ED](_:Int,_:Int,_:Row,numberToVertexId _,rowToType[ED] _)
    LoaderConfiguration(defaultEdgeProvider,vertexDefaultValue[VD]())
  }
}

package ml.sparkling.graph.loaders.graphml

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
object GraphFromGraphML {
  val log = Logger.getLogger(getClass.getName)
  
  type GraphProperties=Map[String,Any]

  object GraphML extends FromPathLoader[GraphProperties,GraphProperties]{
    def apply(path:String):GraphLoader[GraphProperties,GraphProperties]=GraphMLGraphLoader(path)
    def apply(path:URL):GraphLoader[GraphProperties,GraphProperties]=this(path.toString)
  }


  case class GraphMLGraphLoader(path:String) extends GraphLoader[GraphProperties,GraphProperties]{
    override def load(parameters: List[Parameter])(implicit sc:SparkContext): Graph[GraphProperties,GraphProperties] = {
      GraphMLLoader.loadGraphFromML(path)
    }
  }

}

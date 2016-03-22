package ml.sparkling.graph.api.loaders

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object GraphLoading {
  trait GraphLoader[VD,ED]{
    def load(parameters:List[Parameter])(implicit sc:SparkContext):Graph[VD,ED]
  }

  trait TypedGraphLoader[VD2,ED2] extends GraphLoader[VD2,ED2]{
    def load[VD:ClassTag,ED:ClassTag](parameters:List[Parameter])(implicit sc:SparkContext):Graph[VD,ED]
  }
  
  trait FromPathLoader[VD,ED] {
    def apply(path:String):GraphLoader[VD,ED]
  }
  
  object LoadGraph{
    def from[VD:ClassTag,ED:ClassTag](graphLoader: GraphLoader[VD,ED]):GraphLoaderConfigurator[VD,ED]={
      GraphLoaderConfigurator(List.empty,graphLoader)
    }
  }
  
  case  class GraphLoaderConfigurator[VD:ClassTag,ED:ClassTag](parameters:List[Parameter], loader:GraphLoader[_,_]){
    def using(parameter:Parameter)={
      GraphLoaderConfigurator[VD,ED](parameter::parameters,loader)
    }
    
    def load[VD:ClassTag,ED:ClassTag]()(implicit sc:SparkContext): Graph[VD,ED] ={
      loader match{
        case typed:TypedGraphLoader[_,_]=>typed.load[VD,ED](parameters)
        case normal:GraphLoader[VD @unchecked,ED @unchecked] => normal.load(parameters)
        case _ => ???
      }
    }

  }

  trait Parameter
  
  trait WithValueParameter[V] extends Parameter{
    def value:V
  }


}

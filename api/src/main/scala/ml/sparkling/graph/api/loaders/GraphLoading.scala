package ml.sparkling.graph.api.loaders

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object GraphLoading {
  trait GraphLoader{
    def load[VD:ClassTag,ED:ClassTag](parameters:List[Parameter])(implicit sc:SparkContext):Graph[VD,ED]
  }
  
  trait FromPathLoader {
    def apply(path:String):GraphLoader
  }
  
  object LoadGraph{
    def from[VD:ClassTag,ED:ClassTag](graphLoader: GraphLoader):GraphLoaderConfigurator[VD,ED]={
      GraphLoaderConfigurator(List.empty,graphLoader)
    }
  }
  
  case  class GraphLoaderConfigurator[VD:ClassTag,ED:ClassTag](parameters:List[Parameter], loader:GraphLoader){
    def using(parameter:Parameter)={
      GraphLoaderConfigurator[VD,ED](parameter::parameters,loader)
    }
    
    def load[VD:ClassTag,ED:ClassTag]()(implicit sc:SparkContext): Graph[VD,ED] ={
      loader.load[VD,ED](parameters)
    }
  }

  trait Parameter
  
  trait WithValueParameter[V] extends Parameter{
    def value:V
  }


}

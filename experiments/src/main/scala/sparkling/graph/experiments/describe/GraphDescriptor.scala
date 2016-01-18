package sparkling.graph.experiments.describe

import org.apache.spark.graphx.Graph
import sparkling.graph.api.operators.measures.VertexMeasureConfiguration

import scala.reflect.ClassTag

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object GraphDescriptor {

  implicit class DescribeGraph[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED]){

    def describeGraph()(implicit num:Numeric[ED]):Graph[List[Any], ED] ={
      describeGraph(VertexMeasureConfiguration())
    }

    def describeGraph(vertexMeasureConfiguration: VertexMeasureConfiguration[VD,ED])(implicit num:Numeric[ED]):Graph[List[Any], ED]={
      FullGraphDescriptor.describeGraph(graph,vertexMeasureConfiguration)
    }

    def describeGraphToDirectory(directory:String)(implicit num:Numeric[ED]):Unit={
      describeGraphToDirectory(directory,VertexMeasureConfiguration())
    }

    def describeGraphToDirectory(directory:String,vertexMeasureConfiguration: VertexMeasureConfiguration[VD,ED])(implicit num:Numeric[ED]):Unit={
      FullGraphDescriptor.describeGraphToDirectory(graph,directory,vertexMeasureConfiguration)
    }
  }
}

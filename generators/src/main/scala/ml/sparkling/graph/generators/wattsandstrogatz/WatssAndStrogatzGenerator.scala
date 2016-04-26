package ml.sparkling.graph.generators.wattsandstrogatz

import ml.sparkling.graph.api.generators.{GraphGenerator, GraphGeneratorConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import scala.collection.immutable.IndexedSeq

/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 26.04.16.
  */
object WatssAndStrogatzGenerator extends GraphGenerator[WatssAndStrogatzGeneratorConfiguration,Int,Int]{
  override def generate(configuration: WatssAndStrogatzGeneratorConfiguration)(implicit ctx: SparkContext): Graph[Int, Int] = {
    val vertexTuples =ctx
      .parallelize((0l to configuration.numberOfNodes-1))
      .flatMap(vId=>{
        val moves=1l to configuration.meanDegree/2   toList
        val next=moves.map(move=>(vId,(vId+move)%configuration.numberOfNodes))
        val previous=moves.map(move=>{
          val previousNode=vId-move
          if(previousNode<0)
            (vId,configuration.numberOfNodes+previousNode+1)
          else
            (vId,previousNode)
        })
        next ::: previous
      }).distinct()

      

    Graph.fromEdgeTuples(vertexTuples,1)
  }
}
case class WatssAndStrogatzGeneratorConfiguration(val numberOfNodes:Long,val meanDegree:Long,val rewiringProbability:Double) extends GraphGeneratorConfiguration

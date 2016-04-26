package ml.sparkling.graph.generators.ring

import ml.sparkling.graph.api.generators.{GraphGenerator, GraphGeneratorConfiguration}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 26.04.16.
  */
object RingGenerator  extends GraphGenerator[RingGeneratorConfiguration,Int,Int]{
  override def generate(configuration: RingGeneratorConfiguration)(implicit ctx:SparkContext): Graph[Int, Int] = {
    val vertexTuples: RDD[(Long, Long)] =ctx
      .parallelize((0l to configuration.numberOfNodes-1))
      .flatMap(vId=>{
        val nextId=(vId+1) % configuration.numberOfNodes
        println(nextId)
        val previousId=if(vId-1 < 0) {configuration.numberOfNodes-1} else {vId-1}
        (vId,nextId) :: {if(configuration.undirected) List((vId,previousId)) else Nil}
      }
      )
    Graph.fromEdgeTuples(vertexTuples,1)
  }
}
case class RingGeneratorConfiguration(val numberOfNodes:Long, val undirected:Boolean=false) extends GraphGeneratorConfiguration;

package ml.sparkling.graph.generators.wattsandstrogatz

import ml.sparkling.graph.api.generators.RandomNumbers.{RandomNumberGeneratorProvider}
import ml.sparkling.graph.api.generators.{GraphGenerator, GraphGeneratorConfiguration, RandomNumbers}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph


/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 26.04.16.
  */
object WatssAndStrogatzGenerator extends GraphGenerator[WatssAndStrogatzGeneratorConfiguration, Int, Int] {
  override def generate(configuration: WatssAndStrogatzGeneratorConfiguration)(implicit ctx: SparkContext): Graph[Int, Int] = {
    val vertexTuples = ctx
      .parallelize((0l to configuration.numberOfNodes - 1))
      .flatMap(vId => {
        val moves = 1l to configuration.meanDegree / 2 toList
        val next = moves.map(move => (vId, (vId + move) % configuration.numberOfNodes))
        val previous = moves.map(move => (vId, (vId - move+configuration.numberOfNodes)%configuration.numberOfNodes))
        next ::: previous
      }).distinct()

    val rewiredTuples=vertexTuples.zipWithIndex().groupBy(_._1._1).flatMap {
      case (srcId, dataIterable) => {
        val currentDst=dataIterable.map{
          case ((srcId, dstId), index) =>dstId
        }.toList
        val rewiredDst=dataIterable.map {
          case ((srcId, dstId), index) => {
            val generator = configuration.randomNumberGeneratorProvider(index)
            if (generator.nextDouble() < configuration.rewiringProbability) {
              Iterator.continually(generator.nextLong(configuration.numberOfNodes)).find(! currentDst.contains(_)).getOrElse(dstId)
            } else {
              dstId
            }
          }
        }
        rewiredDst.map(dstId=>(srcId,dstId))
      }
    }
    Graph.fromEdgeTuples(rewiredTuples, 1)
  }
}

case class WatssAndStrogatzGeneratorConfiguration(val numberOfNodes: Long,
                                                  val meanDegree: Long,
                                                  val rewiringProbability: Double,
                                                  val randomNumberGeneratorProvider: RandomNumberGeneratorProvider=RandomNumbers.ScalaRandomNumberGeneratorProvider) extends GraphGeneratorConfiguration

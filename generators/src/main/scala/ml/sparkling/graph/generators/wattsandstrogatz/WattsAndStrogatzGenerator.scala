package ml.sparkling.graph.generators.wattsandstrogatz

import ml.sparkling.graph.api.generators.RandomNumbers.RandomNumberGeneratorProvider
import ml.sparkling.graph.api.generators.{GraphGenerator, GraphGeneratorConfiguration, RandomNumbers}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.storage.StorageLevel


/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 26.04.16.
  */
object WattsAndStrogatzGenerator extends GraphGenerator[WattsAndStrogatzGeneratorConfiguration, Int, Int] {
  override def generate(configuration: WattsAndStrogatzGeneratorConfiguration)(implicit ctx: SparkContext): Graph[Int, Int] = {
    val vertexTuples = ctx
      .parallelize((0l to configuration.numberOfNodes - 1), Math.min(Math.max(1,configuration.numberOfNodes / 10), ctx.defaultParallelism).toInt)
      .flatMap(vId => {
        val moves = 1l to configuration.meanDegree / 2 toList
        val next = moves.map(move => (vId, (vId + move) % configuration.numberOfNodes))
        next
      }).distinct()

    val rewiredTuples = vertexTuples.zipWithIndex().groupBy(_._1._1).flatMap {
      case (srcId, dataIterable) => {
        val currentDst = dataIterable.map {
          case ((srcId, dstId), index) => dstId
        }.toList
        val rewiredDst = dataIterable.map {
          case ((srcId, dstId), index) => {
            val generator = configuration.randomNumberGeneratorProvider(index)
            if (generator.nextDouble() < configuration.rewiringProbability &&currentDst.size<configuration.numberOfNodes-1) {
              Iterator.continually(generator.nextLong(configuration.numberOfNodes)).find(newId => !currentDst.contains(newId)&& newId!=srcId).getOrElse(dstId)
            } else {
              dstId
            }
          }
        }
        rewiredDst.map(dstId => (srcId, dstId))
      }
    }.filter(t=>t._1!=t._2)
    val outVertices = if (configuration.mixing) {
      val vertexMix = rewiredTuples.flatMap(t => Iterable(t._1, t._2)).distinct().mapPartitionsWithIndex {
        case (index, data) => {
          val generator = configuration.randomNumberGeneratorProvider(index)
          data.map((id) => (id, generator.nextDouble()))
        }
      }.sortBy(_._2).map(_._1).zipWithIndex()
      rewiredTuples.join(vertexMix).map {
        case (src, (dst, newSrc)) => (dst, newSrc)
      }.join(vertexMix).map {
        case (dst, (newSrc, newDst)) => (newSrc, newDst)
      }
    } else {
      rewiredTuples
    };
    Graph.fromEdgeTuples(outVertices, 1,edgeStorageLevel = configuration.storageLevel,vertexStorageLevel = configuration.storageLevel)
  }
}

case class WattsAndStrogatzGeneratorConfiguration(val numberOfNodes: Long,
                                                  val meanDegree: Long,
                                                  val rewiringProbability: Double, mixing: Boolean = false,
                                                  val randomNumberGeneratorProvider: RandomNumberGeneratorProvider = RandomNumbers.ScalaRandomNumberGeneratorProvider,
                                                  val storageLevel:StorageLevel=StorageLevel.MEMORY_AND_DISK) extends GraphGeneratorConfiguration

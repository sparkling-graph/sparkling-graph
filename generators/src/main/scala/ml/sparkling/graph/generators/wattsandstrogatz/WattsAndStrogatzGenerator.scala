package ml.sparkling.graph.generators.wattsandstrogatz

import ml.sparkling.graph.api.generators.RandomNumbers.RandomNumberGeneratorProvider
import ml.sparkling.graph.api.generators.{GraphGenerator, GraphGeneratorConfiguration, RandomNumbers}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
 * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 26.04.16.
 */
object WattsAndStrogatzGenerator extends GraphGenerator[WattsAndStrogatzGeneratorConfiguration, Int, Int] {
  override def generate(configuration: WattsAndStrogatzGeneratorConfiguration)(implicit ctx: SparkContext): Graph[Int, Int] = {
    val partitions: Int = if (configuration.partitions == -1) ctx.defaultParallelism else configuration.partitions
    val numberOfNodes = configuration.numberOfNodes;
    val vertexTuples: RDD[List[(Long, Long)]] = ctx
      .parallelize((0l to configuration.numberOfNodes - 1), partitions)
      .map(vId => {
        val moves = 1l to configuration.meanDegree toList
        val next = moves.map(move => (vId, (vId + move) % numberOfNodes))
        next
      })
    val rewiredTuples: RDD[(Long, Long)] = vertexTuples.mapPartitionsWithIndex(
      (index, data) => {
        val generator = configuration.randomNumberGeneratorProvider(index)
        data.flatMap(dataIterable => {
          val currentDst = dataIterable.map {
            case (_, dstId) => dstId
          }.toSet
          val mappingData=dataIterable.map(t=>(generator.nextDouble(),t)).map{
            case (prob,data)=> ((prob< configuration.rewiringProbability && currentDst.size<configuration.numberOfNodes-1) ,data)
          }
          val notRewired=mappingData.filter{
            case (prob ,_)=> !prob
          }.map{
            case (_,data)=>data
          }.toSet
          val toRewire=mappingData.filter{
            case (prob ,_)=> prob
          }.map{
            case (_,data)=>data
          }
          notRewired++toRewire.foldLeft(Set[(Long,Long)]()) {
            case (aqq,(srcId, dstId)) => {
              val newDst =Iterator.continually(generator.nextLong(configuration.numberOfNodes))
                .find(newId => !aqq.contains((srcId,newId)) && !notRewired.contains((srcId,newId)) && newId != srcId)
                .getOrElse(dstId)
              aqq + ((srcId, newDst))
            }
          }
        })
      }).persist(configuration.storageLevel)
    val outVertices: RDD[(Long, Long)] = if (configuration.mixing) {
      val vertexMix = ctx.parallelize((0l to configuration.numberOfNodes - 1), partitions).mapPartitionsWithIndex {
        case (index, data) => {
          val generator = configuration.randomNumberGeneratorProvider(index)
          data.map((id) => (id, generator.nextDouble()))
        }
      }.sortBy(_._2).map(_._1).zipWithIndex()
      rewiredTuples.join(vertexMix).map {
        case (_, (dst, newSrc)) => (dst, newSrc)
      }.join(vertexMix).map {
        case (_, (newSrc, newDst)) => (newSrc, newDst)
      }
    } else {
      rewiredTuples
    };
    val edges = outVertices.map {
      case (v1, v2) => Edge(v1, v2, 0)
    }.persist(configuration.storageLevel)
    edges.foreachPartition(_ => ())
    val out = Graph.fromEdges(edges, 0, edgeStorageLevel = configuration.storageLevel, vertexStorageLevel = configuration.storageLevel)
    out
  }
}

case class WattsAndStrogatzGeneratorConfiguration(val numberOfNodes: Long,
                                                  val meanDegree: Long,
                                                  val rewiringProbability: Double, mixing: Boolean = true,
                                                  val randomNumberGeneratorProvider: RandomNumberGeneratorProvider = RandomNumbers.ScalaRandomNumberGeneratorProvider,
                                                  val storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
                                                  val partitions: Int = -1) extends GraphGeneratorConfiguration

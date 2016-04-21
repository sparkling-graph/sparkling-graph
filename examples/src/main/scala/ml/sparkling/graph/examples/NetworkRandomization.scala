package ml.sparkling.graph.examples

import ml.sparkling.graph.api.loaders.GraphLoading.LoadGraph
import ml.sparkling.graph.loaders.csv.GraphFromCsv.CSV
import ml.sparkling.graph.loaders.csv.GraphFromCsv.LoaderParameters.{Delimiter, NoHeader, Partitions, Quotation}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by riomus on 20.04.16.
  */
object NetworkRandomization {
  def main(args:Array[String])= {
    val sparkConf = new SparkConf().setAppName("network-randomization").set("spark.app.id", "sparkling-graph-example")
    implicit val ctx = new SparkContext(sparkConf)
    val path=args(0)
    val pathEmd=args(1)
    val out=args(2)
    val loadPartitions=args(3).toInt
    val graphPartitions=args(4).toInt
    val graph:Graph[String,String]=LoadGraph.from(CSV(path))
      .using(NoHeader)
      .using(Delimiter(","))
      .using(Partitions(loadPartitions))
      .using(Quotation("\"")).load[String,String]().partitionBy(PartitionStrategy.EdgePartition2D,graphPartitions)
    val emd=ctx.textFile(pathEmd,loadPartitions).map(_.split(",").map(v=>v.replaceAll("\"",""))).map(r=>(r.head.toLong,r.tail))
    val srcIdsBase: RDD[VertexId] =graph.edges.map(e=>e.srcId)
    val dstIdsBase=graph.edges.map(e=>e.dstId)
    val saltDst = 23456789L ;
    val saltSrc = 123456789L ;

    def randomize(srcIds:RDD[VertexId],dstIds:RDD[VertexId])= {
      val randomizedSrc = srcIds.mapPartitionsWithIndex((id, itr) => {
        val random = new Random(saltSrc + id)
        itr.map(vId => (random.nextLong(), vId))
      }).sortByKey().zipWithIndex().map(t => (t._2, t._1._2))
      val randomizedDst = dstIds.mapPartitionsWithIndex((id, itr) => {
        val random = new Random(saltDst + id)
        itr.map(vId => (random.nextLong(), vId))
      }).sortByKey().zipWithIndex().map(t => (t._2, t._1._2))
      randomizedSrc.join(randomizedDst).map {
        case (index, (src, dst)) => new Edge[Int](src, dst, 1)
      }
    }
    var numOfSame= -1l
    var lastNumOfSame= -2l
    var randomizedEdges=randomize(srcIdsBase,dstIdsBase)
    var withSame=randomizedEdges.filter(t=> t.srcId == t.dstId)
    while((numOfSame!=lastNumOfSame) && (!withSame.isEmpty())){
      val withoutSame=randomizedEdges.filter(t=>t.srcId!=t.dstId)
      val newRandomized=randomize(withSame.map(_.srcId),withSame.map(_.dstId))
      randomizedEdges=withoutSame.union(newRandomized)
      withSame=newRandomized.filter(e=>e.srcId==e.dstId)
      lastNumOfSame=numOfSame
      numOfSame=withSame.count()
    }

    val randomizedGraph= Graph(graph.vertices,randomizedEdges)

    randomizedGraph.outerJoinVertices(emd)((vId,old,newValue)=>newValue.getOrElse(((0 to 4).map(n=>"0").toArray))).triplets.map(
      edge=>{
        (edge.srcId.toString :: edge.dstId.toString :: edge.srcAttr.toList :::  edge.dstAttr.toList).mkString(",")
      }
    ).saveAsTextFile(out)
  }

}

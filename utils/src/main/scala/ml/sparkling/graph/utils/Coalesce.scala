package ml.sparkling.graph.utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 18.07.16.
  */
object Coalesce {

  def coalesceGraph[VD:ClassTag,ED:ClassTag](graph:Graph[VD,ED],seed:Long=1)(implicit sc:SparkContext):Graph[None.type,ED]={
    val toNewIdMap: RDD[(Long, Long)] = graph.triplets.groupBy(_.dstId).flatMap{
      case (vId,iter)=>{
        val minId=Math.min(vId,iter.map(_.srcId).min)
        (Iterator(vId) ++ iter.map(_.srcId)).map(id=>(id,minId))
      }
    }
    val toNewIdMapRandomised=toNewIdMap.mapPartitionsWithIndex{case (index,valueIterator)=>{
      val generator=new Random(index+seed)
      valueIterator.map(v=>(generator.nextDouble(),v))
    }}.sortByKey()
      .map{case (randomId,value)=>value}
      .reduceByKey((l1,l2)=>l1)
      .collect()  // Fixme: EXECUTION HAPPENS ON DRIVER! HOW TO FIX THAT?
      .foldLeft(Map[Long,Long]()){
      case (agg,(key,value))=>
        if((agg.contains(value) && agg(value)!=value) || agg.values.exists(_==key))
          agg
        else
          agg + (key -> value)
    }
    val toNewIdMapParalelised=sc.parallelize(toNewIdMapRandomised.toList)
    val newEdges=graph.edges.map(e=>(e.srcId,e)).leftOuterJoin(toNewIdMapParalelised).map{
      case (oldSrc,(edge,Some(newSrc)))=>(edge.dstId,(edge,newSrc))
      case (oldSrc,(edge,None))=>(edge.dstId,(edge,oldSrc))
    }.leftOuterJoin(toNewIdMapParalelised).map{
      case (oldDst,((edge,newSrc),Some(newDst)))=>Edge(newSrc,newDst,edge.attr)
      case (oldDst,((edge,newSrc),None))=>Edge(newSrc,oldDst,edge.attr)
    }
    val newEdgesFiltered=newEdges.filter(e=>e.srcId!=e.dstId)
    val vertices=toNewIdMapParalelised.map{case (from,to)=>to}.distinct().map(n=>(n,None))
    Graph(VertexRDD(vertices),newEdgesFiltered,None)
  }
}

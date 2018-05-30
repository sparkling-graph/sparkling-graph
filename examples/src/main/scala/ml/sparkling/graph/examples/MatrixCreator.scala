package ml.sparkling.graph.examples

import java.io.File

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
  */
object MatrixCreator extends Serializable {
  @transient val logger=Logger.getLogger(this.getClass)
  def main(args: Array[String]) = {


    val usage =
      s"""
          Application used to create matrix from AAPSP and APSP output
    Usage:  [--app-name string(${this.getClass.getName})] [--vectorSize long(147478)] [--delimiter string(;)]  [--checkpoint string(/tmp)]  [--partitions int(auto)] inputPath outputPath
    """
    if (args.length == 0) {
      println(usage)
      System.exit(1)
    }
    val optionsMap = Map( ('appName -> this.getClass.getName),('vectorSize->147478),('delimiter->";"),('tupleDelimiter->":"),('checkpoint->"/tmp"),('partitions->None))

    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      def isNotSwitch(s: String) = (s(0) != '-')
      list match {
        case Nil => map
        case "--app-name" :: value :: tail =>
          nextOption(map ++ Map('appName -> value), tail)
        case "--vectorSize" :: value :: tail =>
          nextOption(map ++ Map('vectorSize -> value.toLong), tail)
        case "--delimiter" :: value :: tail =>
          nextOption(map ++ Map('delimiter -> value.toString), tail)
        case "--tupleDelimiter" :: value :: tail =>
          nextOption(map ++ Map('tupleDelimiter -> value.toString), tail)
        case "--checkpoint" :: value :: tail =>
          nextOption(map ++ Map('checkpoint -> value.toString), tail)
        case "--partitions" :: value :: tail =>
          nextOption(map ++ Map('partitions -> Some(value.toInt)), tail)
        case inPath :: outPath :: Nil => map ++ Map('inputPath -> inPath) ++ Map('outputPath -> outPath)
        case option :: tail => println("Unknown option " + option)
          System.exit(1);
          ???
      }
    }

    val options = nextOption(optionsMap, args.toList)
    val in = options('inputPath).asInstanceOf[String]
    val out = options('outputPath).asInstanceOf[String]
    val name = options('appName).asInstanceOf[String]
    val delimiter = options('delimiter).asInstanceOf[String]
    val tupleDelimiter = options('tupleDelimiter).asInstanceOf[String]
    val checkpoint = options('checkpoint).asInstanceOf[String]
    val partitions = options('partitions).asInstanceOf[Option[Int]]
    val vectorSize = options('vectorSize).asInstanceOf[Int]

    logger.info("Running app sparkling-graph-example")
    val sparkConf = new SparkConf().setAppName(name).set("spark.app.id", "sparkling-graph-example")
    val ctx = new SparkContext(sparkConf)
    ctx.setCheckpointDir(checkpoint)

    val parts:List[File]=new File(in).listFiles.filter(f=>f.getName!="index"&&f.getName.startsWith("from")&&f.isDirectory).toList

     parts match{
      case  head::tail=>{
        val startData=loadWithPartitions(ctx, head,partitions).map(s=>s.split(delimiter).toList).map{
          case head::rest=>{
            val data =rest
           (head.toDouble.toInt,data)
          }
          case _ => throw new RuntimeException("Incorrect data!")
        }.cache()
        logger.info(s"Files to process ${tail.length}")
        val outData=tail.zipWithIndex.foldLeft(startData){
          case (data,(file,index))=>{
            logger.info(s"Processing file ${index}")
            val loadedData=loadWithPartitions(ctx, file,partitions).map(s=>s.split(delimiter).toList).map{
              case head::tail =>(head.toDouble.toInt,tail)
              case _ => throw new RuntimeException("Incorrect data!")
            }.cache()
            val out=data.fullOuterJoin(loadedData).map{
              case (id,(Some(d1),Some(d2)))=>(id,d1:::d2)
              case (id,(Some(d1),None))=>(id,d1)
              case (id,(None,Some(d2)))=>(id,d2)
            }.cache()
            if(index%20==0){
              out.checkpoint()
              out.foreachPartition((_)=>{})
            }
            out
        }}


        outData.map{
          case (id,data)=>{
            val dataString=StringBuilder.newBuilder
            val transformedData=stringToList(tupleDelimiter,data)
            val sortedData=transformedData.sortBy(_._1)
            for (i <- 0 to vectorSize){
              if(sortedData(i)._1==i){
                dataString.append(s"${sortedData(i)._2}$delimiter")
              }else{
                dataString.append("0;")
              }
            }
            s"$id$delimiter${dataString.toString()}"

          }
        }.saveAsTextFile(out)
      }
      case _=>logger.error("Not enaught data to create matrix!")
    }

  }


  def loadWithPartitions(ctx: SparkContext, file: File,partitions:Option[Int]): RDD[String] = {
    partitions.map((p)=>{
      ctx.textFile(file.getAbsolutePath,minPartitions=p)
    }).getOrElse(ctx.textFile(file.getAbsolutePath)).persist(StorageLevel.MEMORY_AND_DISK_SER)

  }

  def stringToList(tupleDelimiter: String, rest: List[String]): List[(Int, Long)] = {
    rest.map(s => {
      val splited = s.split(tupleDelimiter)
      (splited(0).toDouble.toInt, splited(1).toDouble.toLong)
    })
  }

}

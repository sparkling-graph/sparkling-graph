package ml.sparkling.graph.examples

import java.io.File

import org.apache.log4j.Logger
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
    Usage:  [--app-name string(${this.getClass.getName})] [--vectorSize long(147478)] [--delimiter string(;)] inputPath outputPath
    """
    if (args.length == 0) {
      println(usage)
      System.exit(1)
    }
    val optionsMap = Map( ('appName -> this.getClass.getName),('vectorSize->147478),('delimiter->";"),('tupleDelimiter->":"))

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
    val vectorSize = options('vectorSize).asInstanceOf[Int]

    logger.info("Running app sparkling-graph-example")
    val sparkConf = new SparkConf().setAppName(name).set("spark.app.id", "sparkling-graph-example")
    val ctx = new SparkContext(sparkConf)

    val parts:List[File]=new File(in).listFiles.filter(f=>f.getName!="index"&&f.getName.startsWith("from")&&f.isDirectory).toList

     parts match{
      case  head::tail=>{
        val startData=ctx.textFile(head.getAbsolutePath).map(s=>s.split(delimiter).toList).flatMap{
          case head::rest=>{
            val data: Array[Long] = dataToVector(tupleDelimiter, vectorSize, rest)
            Some((head.toLong,data))
          }
          case _=>None
        }
        val outData=tail.foldLeft(startData)((data,file)=>{
          val loadedData=ctx.textFile(file.getAbsolutePath).map(s=>s.split(delimiter).toList).flatMap[(Long,List[String])]{
            case head::tail =>Some((head.toLong,tail))
            case _ => None
          }
          val out=data.fullOuterJoin(loadedData).flatMap{
            case (id,(Some(vector),Some(data)))=>{
              val outVector=data.map(t => t.split(tupleDelimiter)).map((t) => (t(0).toInt, t(1).toLong)).foldLeft(vector)((buffer, tuple) => {
                buffer(tuple._1) = tuple._2
                buffer
              })
              Some((id,outVector))
            }
            case (id,(Some(vector),None))=>Some((id,vector))
            case (id,(None,Some(data)))=>{
              val vector: Array[Long] = dataToVector(tupleDelimiter, vectorSize, data)
              Some((id,vector))
            }
            case (id,(None,None))=>None
          }
          out.localCheckpoint()
          out.foreachPartition((_)=>{})
          out
        })
        outData.map{
          case (id,data)=>s"$id$delimiter${data.mkString(delimiter)}"
        }.saveAsTextFile(out)
      }
      case _=>logger.error("Not enaught data to create matrix!")
    }

  }


  def dataToVector(tupleDelimiter: String, vectorSize: Int, rest: List[String]): Array[Long] = {
    val buffer = Array.ofDim[Long](vectorSize)
    val data = rest.map(t => t.split(tupleDelimiter)).map((t) => (t(0).toInt, t(1).toLong)).foldLeft(buffer)((buffer, tuple) => {
      buffer(tuple._1) = tuple._2;
      buffer
    })
    data
  }
}

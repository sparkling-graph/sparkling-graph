package ml.sparkling.graph.examples

import ml.sparkling.graph.loaders.csv.providers.PropertyProviders
import ml.sparkling.graph.loaders.csv.utils.DefaultTransformers
import ml.sparkling.graph.loaders.csv.{CSVLoader, CsvLoaderConfig}
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Graph, PartitionStrategy}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
  */
abstract class ExampleApp extends Serializable {
  @transient val logger=Logger.getLogger(this.getClass)
  var file, out, delimiter, name: String = "";
  var partitionNumber, graphPartitions, edgeField: Int = 0;
  var treatAsUndirected: Boolean = false;
  var withIndexing: Boolean = true;
  var bucketSize: Long = 0l;
  var partitionedGraph: Graph[String, Double] = null;
  var master: Option[String] = None
  implicit var ctx: SparkContext = null;

  def main(args: Array[String]) = {

    val usage =
      s"""
    Usage:  [--app-name string(${this.getClass.getName})] [--delimiter string(;)]
            [--load-partitions int(auto)] [--graph-partitions int(auto)]
            [--bucket-size int(-1)] [--treat-as-undirected boolean(false)] [--edge-field int(2)]
            [--master string(None)] [--withIndexing Boolean(true)]
            [--checkpoint-dir string(TMPDIR)]
            inputFile outputFile
    """
    if (args.length == 0) {
      println(usage)
      System.exit(1)
    }
    val optionsMap = Map(('indexing->true),('master -> None), ('appName -> this.getClass.getName),
      ('edgeField -> 2), ('delimiter -> ";"), ('loadPartitions -> -1), ('graphPartitions -> -1),
      ('bucketSize -> -1l), ('treatAsUndirected -> false),('checkpointDir->System.getProperty("java.io.tmpdir")))

    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--delimiter" :: value :: tail =>
          nextOption(map ++ Map('delimiter -> value), tail)
        case "--load-partitions" :: value :: tail =>
          nextOption(map ++ Map('loadPartitions -> value.toInt), tail)
        case "--edge-field" :: value :: tail =>
          nextOption(map ++ Map('edgeField -> value.toInt), tail)
        case "--graph-partitions" :: value :: tail =>
          nextOption(map ++ Map('graphPartitions -> value.toInt), tail)
        case "--app-name" :: value :: tail =>
          nextOption(map ++ Map('appName -> value), tail)
        case "--bucket-size" :: value :: tail =>
          nextOption(map ++ Map('bucketSize -> value.toLong), tail)
        case "--treat-as-undirected" :: value :: tail =>
          nextOption(map ++ Map('treatAsUndirected -> value.toBoolean), tail)
        case "--master" :: value :: tail =>
          nextOption(map ++ Map('master -> Option(value)), tail)
        case "--withIndexing" :: value :: tail =>
          nextOption(map ++ Map('indexing -> value.toBoolean), tail)
        case "--checkpoint-dir" :: value :: tail =>
          nextOption(map ++ Map('checkpointDir -> value.toString), tail)
        case inFile :: outFile :: Nil => map ++ Map('inputFile -> inFile) ++ Map('outputFile -> outFile)
        case option :: tail => println("Unknown option " + option)
          System.exit(1);
          ???
      }
    }

    val options = nextOption(optionsMap, args.toList)
    val checkpointDir=options('checkpointDir).asInstanceOf[String]
    file = options('inputFile).asInstanceOf[String]
    out = options('outputFile).asInstanceOf[String]
    delimiter = options('delimiter).asInstanceOf[String]
    partitionNumber = options('loadPartitions).asInstanceOf[Int]
    graphPartitions = options('graphPartitions).asInstanceOf[Int]
    treatAsUndirected = options('treatAsUndirected).asInstanceOf[Boolean]
    bucketSize = options('bucketSize).asInstanceOf[Long]
    edgeField = options('edgeField).asInstanceOf[Int]
    name = options('appName).asInstanceOf[String]
    master = options('master).asInstanceOf[Option[String]]
    withIndexing = options('indexing).asInstanceOf[Boolean]

    logger.info("Running app sparkling-graph-example")
    val sparkConf = new SparkConf().setAppName(name).set("spark.app.id", "sparkling-graph-example")
    ctx = new SparkContext(master.map(m => sparkConf.setMaster(m)).getOrElse(sparkConf))
    ctx.setCheckpointDir(checkpointDir)

    val graph =
      if(withIndexing) {
        if (partitionNumber != -1)
          CSVLoader.loadGraphFromCSVWitVertexIndexing[String, Double](file,
            new CsvLoaderConfig(delimiter = delimiter),
            edgeAttributeProvider = Utils.getEdgeAttributeProvider(edgeField), partitions = partitionNumber)
        else
          CSVLoader.loadGraphFromCSVWitVertexIndexing[String, Double](file,
            new CsvLoaderConfig(delimiter = delimiter), edgeAttributeProvider = Utils.getEdgeAttributeProvider(edgeField))
      }
    else{
        if (partitionNumber != -1)
          CSVLoader.loadGraphFromCSV(file,
            new CsvLoaderConfig(delimiter = delimiter),
            edgeAttributeProvider = Utils.getEdgeAttributeProvider(edgeField), partitions = partitionNumber)
        else
          CSVLoader.loadGraphFromCSV(file,
            new CsvLoaderConfig(delimiter = delimiter), edgeAttributeProvider = Utils.getEdgeAttributeProvider(edgeField))
      }
    partitionedGraph = if (graphPartitions != -1) {
      graph.partitionBy(PartitionStrategy.EdgePartition2D, graphPartitions).cache()
    } else {
      graph.partitionBy(PartitionStrategy.EdgePartition2D).cache()
    }

    body()

    if(!ctx.isStopped){
      ctx.stop()
    }
  }


  def body()
}

private object Utils {
  def getEdgeAttributeProvider(edgeField: Int) = {
    if (edgeField == -1) DefaultTransformers.defaultEdgeAttribute _ else PropertyProviders.doubleAttributeProvider(edgeField) _
  }
}

package ml.sparkling.graph.loaders.graphml

import com.databricks.spark.xml._
import ml.sparkling.graph.loaders.graphml.GraphMLTypes.TypeHandler
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object GraphMLLoader {
  type ValuesMap=Map[String,Any]
  val databricksXmlFormat: String = "com.databricks.spark.xml"

  case class GraphMLAttribute(name:String,handler:TypeHandler)

  /**
   * Method loads single graph from graph
   * Currently spark-xml is not suporting self closing tags. Because of that XML loading can be memmory consuming (on driver node)
   * @param path - path of XML file
   * @param sc - spark context
   * @return loaded graph
   */
  def loadGraphFromML(path: String)(implicit sc: SparkContext): Graph[ValuesMap, ValuesMap] = {
    val sqlContext = new SQLContext(sc)

    val graphDataFrame = sqlContext.xmlFile(path,rowTag = "graph",failFast = true)
    graphDataFrame.collect()
    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!! graphdata")
//    val graphMLSchema = StructType(Array(
//      StructField("key", ArrayType(StructType(Array(
//        StructField("@attr.name",StringType,nullable = true),
//        StructField("@attr.type",StringType,nullable = true),
//        StructField("@for",StringType,nullable = true),
//          StructField("@id",StringType,nullable = true)
//      ))), nullable = true)))

    val nodesKeys = sqlContext.xmlFile(path,rowTag = "graphml",failFast = true)
      .flatMap(r=>Try(r.getAs[mutable.WrappedArray[Row]]("key").toArray).getOrElse(Array.empty))
      .filter(r=>r.getAs[String]("@for")=="node")
nodesKeys.collect()
println("!!!!!!!!!!!!!!!!!!!!!!!!!!!! nodeskeys")
    val attrHandler=nodesKeys
      .map(r=>(r.getAs[String]("@id"),GraphMLAttribute(r.getAs[String]("@attr.name"),GraphMLTypes(r.getAs[String]("@attr.type"))))).collect().toMap

    val verticesWithData = graphDataFrame.flatMap(r => r.getAs[Any]("node") match {
      case data: mutable.WrappedArray[Row @unchecked] => data.array
      case data: Row => Array(data)
    })

    val verticesIndex = verticesWithData.map(r=>r.getAs[String]("@id")).zipWithUniqueId().collect().toMap
    val vertices: RDD[(VertexId, Map[String,Any])] = verticesWithData
      .map(
        r=>(verticesIndex(r.getAs[String]("@id")),Try(r.getAs[mutable.WrappedArray[Row]]("data")).toOption.map(
        _.map(r=>{
          val attribute=attrHandler(r.getAs[String]("@key"))
          (attribute.name,attribute.handler(r.getAs[String]("#VALUE")))
        }).toMap
        ).getOrElse(Map.empty))
      )
    vertices.collect()
    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1 vertices")
    val edgesRows = graphDataFrame.flatMap(r => r.getAs[Any]("edge") match {
      case data: mutable.WrappedArray[Row @unchecked] => data.array
      case data: Row => Array(data)
    })
      .map(r => Edge(verticesIndex(r.getAs[String]("@source")), verticesIndex(r.getAs[String]("@target")), Map[String,Any]("id"->r.getAs[String]("@id"))))
    edgesRows.collect()
    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1 edges")
    Graph(vertices, edgesRows)
  }



}

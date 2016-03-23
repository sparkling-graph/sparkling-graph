package ml.sparkling.graph.loaders.graphml

import com.databricks.spark.xml._
import ml.sparkling.graph.loaders.graphml.GraphMLFormat._
import ml.sparkling.graph.loaders.graphml.GraphMLTypes.TypeHandler
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable
import scala.util.Try

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object GraphMLLoader {
  type ValuesMap = Map[String, Any]
  val databricksXmlFormat: String = "com.databricks.spark.xml"

  case class GraphMLAttribute(name: String, handler: TypeHandler)


  /**
   * Method loads single graph from graph
   * Currently spark-xml is not suporting self closing tags. Because of that XML loading can be memmory consuming (on driver node)
   * @param path - path of XML file
   * @param sc - spark context
   * @return loaded graph
   */
  def loadGraphFromML(path: String)(implicit sc: SparkContext): Graph[ValuesMap, ValuesMap] = {
    println(sc.textFile(path).collect().toList)
    val sqlContext = new SQLContext(sc)
    val graphDataFrame = sqlContext.xmlFile(path, rowTag = graphTag, failFast = true)


    val nodesKeys = sqlContext.xmlFile(path, rowTag = graphMLTag, failFast = true)
      .flatMap(r => Try(r.getAs[mutable.WrappedArray[Row]](keyTag).toArray).getOrElse(Array.empty))
      .filter(r => r.getAs[String](forAttribute) == nodeTag)

    val attrHandler = nodesKeys
      .map(r => (r.getAs[String](idAttribute), GraphMLAttribute(r.getAs[String](nameAttribute), GraphMLTypes(r.getAs[String](typeAttribute)))))
      .collect().toMap

    val verticesWithData = graphDataFrame.flatMap(r => r.getAs[Any](nodeTag) match {
      case data: mutable.WrappedArray[Row@unchecked] => data.array
      case data: Row => Array(data)
    })

    val verticesIndex = verticesWithData.map(r => r.getAs[String](idAttribute)).zipWithUniqueId().collect().toMap

    val vertices: RDD[(VertexId, Map[String, Any])] = verticesWithData
      .map(
        r => (verticesIndex(r.getAs[String](idAttribute)), Try(r.getAs[mutable.WrappedArray[Row]](dataTag)).toOption.map(
          _.map(r => {
            val attribute = attrHandler(r.getAs[String](keyAttribute))
            (attribute.name, attribute.handler(r.getAs[String](tagValue)))
          }).toMap
        ).getOrElse(Map.empty))
      )

    val edgesRows = graphDataFrame.flatMap(r => r.getAs[Any](edgeTag) match {
      case data: mutable.WrappedArray[Row@unchecked] => data.array
      case data: Row => Array(data)
    })
      .map(r => Edge(
        verticesIndex(r.getAs[String](sourceAttribute)),
        verticesIndex(r.getAs[String](targetAttribute)),
        Map[String,Any]("id"->r.getAs[String](idAttribute))
      ))
    Graph(vertices, edgesRows)
  }

}

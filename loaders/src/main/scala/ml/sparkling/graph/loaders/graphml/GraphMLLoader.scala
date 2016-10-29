package ml.sparkling.graph.loaders.graphml

import com.databricks.spark.xml._
import ml.sparkling.graph.loaders.graphml.GraphMLFormat._
import ml.sparkling.graph.loaders.graphml.GraphMLTypes.TypeHandler
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.collection.mutable
import scala.util.Try

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object GraphMLLoader {
  type ValuesMap = Map[String, Any]

  case class GraphMLAttribute(name: String, handler: TypeHandler)


  /**
   * Method loads single graph from XML file with GraphML format.
   * Currently spark-xml is not suporting self closing tags. Because of that XML loading can be memmory consuming (on driver node)
   * @param path - path of XML file
   * @param sc - spark context
   * @return loaded graph
   */
  def loadGraphFromML(path: String)(implicit sc: SparkContext): Graph[ValuesMap, ValuesMap] = {
    val sparkSession=SparkSession.builder().getOrCreate();

    val graphDataFrame = sparkSession.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("attributePrefix","@")
      .option("valueTag","#VALUE")
      .option("rowTag",graphTag).load(path).rdd

    val keys =sparkSession.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("attributePrefix","@")
      .option("valueTag","#VALUE")
      .option("rowTag",graphMLTag).load(path).rdd
      .flatMap(r => Try(r.getAs[mutable.WrappedArray[Row]](keyTag).toArray).getOrElse(Array.empty))

    val nodesKeys = keys
      .filter(r => r.getAs[String](forAttribute) == nodeTag)
    val edgeKeys = keys
      .filter(r => r.getAs[String](forAttribute) == edgeTag)

    val nodeAttrHandlers = createAttrHandlersFor(nodesKeys)
    val edgeAttrHandlers = createAttrHandlersFor(edgeKeys)

    val verticesWithData = graphDataFrame.flatMap(r => r.getAs[Any](nodeTag) match {
      case data: mutable.WrappedArray[Row@unchecked] => data.array
      case data: Row => Array(data)
    })

    val verticesIndex = verticesWithData.map(r => r.getAs[String](idAttribute)).zipWithUniqueId().collect().toMap

    val vertices: RDD[(VertexId, Map[String, Any])] = verticesWithData
      .map(
        r => (verticesIndex(r.getAs[String](idAttribute)), extractAttributesMap(nodeAttrHandlers, r))
      )

    val edgesRows = graphDataFrame.flatMap(r => r.getAs[Any](edgeTag) match {
      case data: mutable.WrappedArray[Row@unchecked] => data.array
      case data: Row => Array(data)
    })
      .map(r => Edge(
        verticesIndex(r.getAs[String](sourceAttribute)),
        verticesIndex(r.getAs[String](targetAttribute)),
        extractAttributesMap(edgeAttrHandlers, r)
      ))
    Graph(vertices, edgesRows)
  }

  def extractAttributesMap(attrHandlers: Map[String, GraphMLAttribute], r: Row): Map[String, Any] = {
    Try(r.getAs[mutable.WrappedArray[Row]](dataTag)).toOption.map(
      _.map(r => {
        val attribute = attrHandlers(r.getAs[String](keyAttribute))
        (attribute.name, attribute.handler(r.getAs[String](tagValue)))
      }).toMap
    ).getOrElse(Map.empty) + ("id" -> r.getAs[String](idAttribute))
  }

  def createAttrHandlersFor(keys: RDD[Row]): Map[String, GraphMLAttribute] = {
    keys
      .map(r => (r.getAs[String](idAttribute), GraphMLAttribute(r.getAs[String](nameAttribute), GraphMLTypes(r.getAs[String](typeAttribute)))))
      .collect().toMap
  }
}

package ml.sparkling.graph.examples

import ml.sparkling.graph.loaders.csv.{CSVLoader, CsvLoaderConfig}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, GroupedData, SQLContext}
import org.apache.spark.sql.types._
;
/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object UserRatingDistribution {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("user-rating-dist").set("spark.app.id", "sparkling-graph-example")
    implicit val ctx = new SparkContext(sparkConf)
    val f1 = args(0)
    val f2 = args(1)
    val out = args(2)
    val partitions=args(3).toInt
    val edge=args(4).toInt

    val d1 = ";"
    val d2 = ","

    val graph = CSVLoader.loadGraphFromCSVWitVertexIndexing(f1, new CsvLoaderConfig(delimiter = d1), defaultVertex = "<NO VERTEX>", edgeAttributeProvider = Utils.getEdgeAttributeProvider(edge), partitions = partitions)

    val sqlContext = new SQLContext(ctx)
    import sqlContext.implicits._
    val csvSchema = StructType(List(
      StructField("author_id", IntegerType, true),
      StructField("user_login", StringType, true),
      StructField("project_id", IntegerType, true),
      StructField("project_name", StringType, true),
      StructField("repo_stars_growth_in_quarter", DoubleType, true),
      StructField("dev_commits_share_in_quarter", DoubleType, true),
      StructField("dev_stars_share", DoubleType, true),
      StructField("dev_new_followers_in_this_quarter_proportional", DoubleType, true),
      StructField("dev_combined_stars_and_followers_rating", DoubleType, true)))

    val dataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(csvSchema)
      .load(f2)

    val meanRating: DataFrame = dataFrame.groupBy("user_login").mean("dev_combined_stars_and_followers_rating")

    val userToRating: RDD[(VertexId, Double)] = meanRating.join(graph.vertices.toDF("user_id", "user_login"), "user_login").rdd.map(r => (r.getAs[VertexId]("user_id"), r.getAs[Double]("avg(dev_combined_stars_and_followers_rating)")))

    val graphWithRatings: Graph[Double, Double] = graph.outerJoinVertices(userToRating)((a, b, c) => c.getOrElse(0.))

    val userNeighboorsRatings = graphWithRatings.aggregateMessages[List[Double]](
    sendMsg = ec=>{
      ec.sendToDst(ec.srcAttr::Nil)
      ec.sendToSrc(ec.dstAttr::Nil)
    }
    ,mergeMsg = (m1,m2)=>m1++m2
    )

    graph
      .outerJoinVertices(graphWithRatings.vertices)((a, b, c) => (b::c.getOrElse(0)::Nil))
      .outerJoinVertices(userNeighboorsRatings)((a, b, c) => (b ++ c.getOrElse(List.empty)))
      .vertices.map(_._2.mkString(";")).saveAsTextFile(out)

  }
}

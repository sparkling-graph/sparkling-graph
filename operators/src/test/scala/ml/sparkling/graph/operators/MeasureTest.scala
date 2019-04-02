package ml.sparkling.graph.operators

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.scalatest._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
abstract class MeasureTest(implicit sc:SparkContext)  extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers{
  def time[T](str: String)(thunk: => T): (T,Long) = {
    logger.info(s"$str...")
    val t1 = System.currentTimeMillis
    val x = thunk
    val t2 = System.currentTimeMillis
    val diff=t2 - t1
    logger.info(s"$diff ms")
    (x,diff)
  }

  val logger=Logger.getLogger(this.getClass)

  def loadGraph(file:String)={
    val out: Graph[Int, Int] =GraphLoader.edgeListFile(sc,file.toString)
    out.vertices.setName(s"Graph vertices ${file}")
    out.edges.setName(s"Graph edges ${file}")
    out.triplets.setName(s"Graph triplets ${file}")
    out
  }

}
package ml.sparkling.graph.operators

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.scalatest._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
abstract class MeasureTest(implicit sc:SparkContext)  extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers with BeforeAndAfterEach{

  val logger=Logger.getLogger(this.getClass)

  def loadGraph(file:String)={
    GraphLoader.edgeListFile(sc,file.toString)
  }


  override def  beforeEach(testData: TestData) = {
    logger.info(s"${Console.GREEN} Running test ${testData.name} ${Console.RESET} ")
  }


}
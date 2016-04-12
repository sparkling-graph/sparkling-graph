package ml.sparkling.graph.operators

import ml.sparkling.graph.operators.partitioning.CommunityBasedPartitioning$Test
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class SparkTest extends Spec with BeforeAndAfterAll {

  val master = "local[*]"

  def appName: String = "operators-tests"

  implicit val sc: SparkContext = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    new SparkContext(conf)
  }

  override def afterAll() = {
    sc.stop()
  }

  override def nestedSuites = {
    Vector(
//      new VertexEmbeddedness$Test,
//      new NeighborhoodConnectivity$Test,
//      new Hits$Test,
//      new EigenvectorCentrality$Test,
//      new LocalClustering$Test,
//      new Closeness$Test,
//      new ShortestPathsAlgorithm$Test,
//    new PSCAN$Test
    new CommunityBasedPartitioning$Test
    )
  }


}
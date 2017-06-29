package ml.sparkling.graph.operators.measures.vertex.betweenness.hua

import java.nio.file.Files

import ml.sparkling.graph.operators.MeasureTest
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
  * Created by mth on 6/29/17.
  */
class BetweennessHua$Test (implicit sc: SparkContext) extends MeasureTest {
  val tempDir = Files.createTempDirectory("spark-checkpoint")

  override def beforeAll() = {
    sc.setCheckpointDir(tempDir.toAbsolutePath.toString)
  }

  override def afterAll() = {
    FileUtils.deleteDirectory(tempDir.toFile)
  }

  "Hua betweenness centrality for random graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/graph_ER_15")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes betweenness")
    val result = new NearlyOptimalBC(graph).computeBC
    Then("Should calculate betweenness correctly")
    val bcFile = getClass.getResource("/graphs/graph_ER_15_bc")
    val bcCorrectValues = sc.textFile(bcFile.getPath)
      .filter(_.length > 0)
      .map(l => { val t = l.split("\t", 2); (t(0).toInt, t(1).toDouble) })
      .sortBy({ case (vId, data) => vId })
      .map({ case (vId, data) => data}).collect()
    val bcValues = result.sortBy({ case (vId, data) => vId })
      .map({ case (vId, data) => data }).collect()
    bcCorrectValues.zip(bcValues).foreach({ case (a, b) =>
      a should be(b +- 1e-5)
    })

    result.unpersist(false)
  }

}
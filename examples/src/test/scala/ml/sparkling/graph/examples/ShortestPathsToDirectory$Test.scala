package ml.sparkling.graph.examples

import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.scalatest._

import scala.io.Source


/**
  * Created by riomus on 21.04.16.
  */
class ShortestPathsToDirectory$Test extends FlatSpec with BeforeAndAfterAll with GivenWhenThen with Matchers {
  val file: Path = Files.createTempDirectory("tempDir")

  override def afterAll() ={
    FileUtils.deleteDirectory(file.toFile)
  }


  "Standard paths" should "be computed correctly" in {
    Given("Using given graph")
    val graph = getClass.getResource("/examples_graphs/5_nodes_directed")
    When("Computes paths")
    ShortestPathsToDirectory.main(Array("--withIndexing", "false","--checkpoint-dir",file.toString,  "--master", "local[*]", "--load-partitions", "1", "--bucket-size", "1000", "--treat-as-undirected", "false", graph.toString, s"${file.toString}/1"))
    Then("Should correctly compute")
    val result: List[String] = Source.fromFile(s"${file.toString}/1/from_1/part-00000").getLines().toList.sortBy(_.split(",")(0))
    result should equal(
      List("1;3:2.0;5:4.0;2:1.0;4:3.0",
        "2;4:2.0;3:1.0;5:3.0",
        "3;4:1.0;5:2.0",
        "4;5:1.0",
        "5;"))
  }

  "Approximated paths" should "be computed correctly" in {
    Given("Using given graph")
    val graph = getClass.getResource("/examples_graphs/5_nodes_directed")
    When("Computes approximated paths")
    ApproximateShortestPathsToDirectory.main(Array("--withIndexing","false","--checkpoint-dir",file.toString, "--master", "local[*]", "--load-partitions", "1", "--bucket-size", "1000", "--treat-as-undirected", "false", graph.toString, s"${file.toString}/2"))
    Then("Should correctly compute")
    val result: List[String] = Source.fromFile(s"${file.toString}/2/from_1/part-00000").getLines().toList.sortBy(_.split(",")(0))
    result should equal(
      List("1;5:12.0;4:9.0;3:6.0;2:1.0",
        "2;4:6.0;5:9.0;3:1.0",
        "3;4:1.0;5:6.0",
        "4;5:1.0",
        "5;"))
  }
}

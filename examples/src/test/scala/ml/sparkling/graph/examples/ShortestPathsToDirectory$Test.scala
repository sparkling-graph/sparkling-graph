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
    ShortestPathsToDirectory.main(Array("--no-ui","--withIndexing", "false","--checkpoint-dir",file.toString,  "--master", "local[*]", "--load-partitions", "1", "--bucket-size", "1000", "--treat-as-undirected", "false", graph.toString, s"${file.toString}/1"))
    Then("Should correctly compute")
    val result: List[String] = Source.fromFile(s"${file.toString}/1/from_1/part-00000").getLines().toList.sortBy(_.split(",")(0))
    result should equal(
      List("1;1:0.0;3:2.0;5:4.0;2:1.0;4:3.0",
        "2;2:0.0;4:2.0;3:1.0;5:3.0",
        "3;3:0.0;4:1.0;5:2.0",
        "4;5:1.0;4:0.0",
        "5;5:0.0"))
  }

  "Approximated paths" should "be computed correctly" in {
    Given("Using given graph")
    val graph = getClass.getResource("/examples_graphs/5_nodes_directed")
    When("Computes approximated paths")
    ApproximateShortestPathsToDirectory.main(Array("--withIndexing","false","--checkpoint-dir",file.toString, "--master", "local[*]", "--load-partitions", "1", "--bucket-size", "1000", "--treat-as-undirected", "false", graph.toString, s"${file.toString}/2"))
    Then("Should correctly compute")
    val result= Source.fromFile(s"${file.toString}/2/from_1/part-00000").getLines().toList.sortBy(_.split(",")(0)).map(_.split(";").toSet)
    result should equal(
      List(
        Set("1:0.0", "2:1.0", "3:2.0", "5:8.0", "1", "4:5.0"),
        Set("2:0.0", "4:2.0", "5:8.0", "2", "3:1.0"),
        Set("3", "5:2.0", "4:1.0", "3:0.0"),
        Set("4", "5:1.0", "4:0.0"),
        Set("5", "5:0.0")
      )
    )
  }
  "Approximated paths with small bucket" should "be computed correctly" in {
    Given("Using given graph")
    val graph = getClass.getResource("/examples_graphs/5_nodes_directed")
    When("Computes approximated paths")
    ApproximateShortestPathsToDirectory.main(Array("--withIndexing","false","--checkpoint-dir",file.toString, "--master", "local[*]", "--load-partitions", "1", "--bucket-size", "1", "--treat-as-undirected", "false", graph.toString, s"${file.toString}/3"))
    Then("Should correctly compute")
    Source.fromFile(s"${file.toString}/3/from_1/part-00000").getLines().toList.sortBy(_.split(",")(0)).map(_.split(";").toSet) should equal(
      List("1;2:1.0;1:0.0", "2;2:0.0", "3;", "4;", "5;").map(_.split(";").toSet))
    Source.fromFile(s"${file.toString}/3/from_3/part-00000").getLines().toList.sortBy(_.split(",")(0)).map(_.split(";").toSet) should equal(
      List("1;4:5.0;3:2.0", "2;4:2.0;3:1.0", "3;4:1.0;3:0.0", "4;4:0.0", "5;").map(_.split(";").toSet))
    Source.fromFile(s"${file.toString}/3/from_5/part-00000").getLines().toList.sortBy(_.split(",")(0)).map(_.split(";").toSet) should equal(
      List("1;5:8.0", "2;5:8.0", "3;5:2.0", "4;5:1.0", "5;5:0.0").map(_.split(";").toSet))
  }
  "Approximated paths with small  bucket as undirected" should "be computed correctly" in {
    Given("Using given graph")
    val graph = getClass.getResource("/examples_graphs/5_nodes_directed")
    When("Computes approximated paths")
    ApproximateShortestPathsToDirectory.main(Array("--withIndexing","false","--checkpoint-dir",file.toString, "--master", "local[*]", "--load-partitions", "1", "--bucket-size", "1", "--treat-as-undirected", "true", graph.toString, s"${file.toString}/4"))
    Then("Should correctly compute")
    Source.fromFile(s"${file.toString}/4/from_1/part-00000").getLines().toList.sortBy(_.split(",")(0)).map(_.split(";").toSet) should equal(
      List("1;2:1.0;1:0.0", "2;1:1.0;2:0.0", "3;2:1.0;1:2.0", "4;2:2.0;1:5.0", "5;2:8.0;1:8.0").map(_.split(";").toSet))
    Source.fromFile(s"${file.toString}/4/from_3/part-00000").getLines().toList.sortBy(_.split(",")(0)).map(_.split(";").toSet) should equal(
      List("1;4:5.0;3:2.0", "2;4:2.0;3:1.0", "3;4:1.0;3:0.0", "4;3:1.0;4:0.0", "5;4:1.0;3:2.0").map(_.split(";").toSet))
    Source.fromFile(s"${file.toString}/4/from_5/part-00000").getLines().toList.sortBy(_.split(",")(0)).map(_.split(";").toSet) should equal(
      List("1;5:8.0", "2;5:8.0", "3;5:2.0", "4;5:1.0", "5;5:0.0").map(_.split(";").toSet))
  }

  "Approximated paths with small bucket for ring" should "be computed correctly" in {
    Given("Using given graph")
    val graph = getClass.getResource("/examples_graphs/5_nodes_ring")
    When("Computes approximated paths")
    ApproximateShortestPathsToDirectory.main(Array("--withIndexing","false","--checkpoint-dir",file.toString, "--master", "local[*]", "--load-partitions", "1", "--bucket-size", "1", "--treat-as-undirected", "true", graph.toString, s"${file.toString}/5"))
    Then("Should correctly compute")
    Source.fromFile(s"${file.toString}/5/from_1/part-00000").getLines().toList.sortBy(_.split(",")(0)).map(_.split(";").toSet) should equal(
      List("1;2:1.0;5:1.0;1:0.0", "2;5:2.0;1:1.0;2:0.0", "3;2:1.0;5:2.0;1:2.0", "4;2:2.0;5:1.0;1:2.0", "5;2:2.0;1:1.0;5:0.0").map(_.split(";").toSet))
    Source.fromFile(s"${file.toString}/5/from_3/part-00000").getLines().toList.sortBy(_.split(",")(0)).map(_.split(";").toSet) should equal(
      List("1;4:2.0;3:2.0", "2;4:2.0;3:1.0", "3;4:1.0;3:0.0", "4;3:1.0;4:0.0", "5;4:1.0;3:2.0").map(_.split(";").toSet))
  }


}

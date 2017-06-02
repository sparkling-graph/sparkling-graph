package ml.sparkling.graph.operators.measures.edge

import ml.sparkling.graph.operators.MeasureTest
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import ml.sparkling.graph.operators.OperatorsDSL._
/**
  * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
  */
class AdamicAdar$Test(implicit sc:SparkContext) extends MeasureTest {


   "Adamic/Adar for star graph" should "be 0 for each node" in{
     Given("graph")
     val filePath = getClass.getResource("/graphs/6_nodes_star")
     val graph:Graph[Int,Int]=loadGraph(filePath.toString)
     When("Computes Adamic/Adar")
     val result=AdamicAdar.computeWithPreprocessing(graph)
     Then("Should calculate Adamic/Adar")
     val resultValues=result.edges.map(_.attr).distinct().collect()
     resultValues(0) should equal(0)
     resultValues.size should equal(1)
     graph.unpersist(true)
   }

   "Adamic/Adar for full graph using DSL" should "be 1.8205 for each node" in{
     Given("graph")
     val filePath = getClass.getResource("/graphs/4_nodes_full")
     val graph:Graph[Int,Int]=loadGraph(filePath.toString)
     When("Computes Adamic/Adar")
     val result=graph.adamicAdar(true)
     Then("Should calculate Adamic/Adar")
     val resultValues=result.edges.map(_.attr).distinct().collect()
     resultValues(0) should equal(1.82047 +- 1e-5)
     resultValues.size should equal(1)
     graph.unpersist(true)
   }


 }

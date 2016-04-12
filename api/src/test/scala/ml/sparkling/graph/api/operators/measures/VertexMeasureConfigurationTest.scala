package ml.sparkling.graph.api.operators.measures

import ml.sparkling.graph.api.operators.IterativeComputation.BucketSizeProvider
import org.apache.spark.graphx.Graph
import org.scalatest.{FlatSpec, GivenWhenThen}


/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class VertexMeasureConfigurationTest extends FlatSpec with GivenWhenThen  {

  "Creation without parameters" should "be possible" in{
    VertexMeasureConfiguration()
  }

  "Creation with undirected flag" should "be possible" in{
    Given("Directed flag")
    val flag=false
    When("Configuration creation")
    VertexMeasureConfiguration(treatAsUndirected = flag )
  }

  "Creation with bucket size provider" should "be possible" in{
    Given("Bucker size provider")
    val provider:BucketSizeProvider[Long,Long]=(g:Graph[Long,Long])=>1l
    When("Configuration creation")
    VertexMeasureConfiguration(bucketSizeProvider = provider)
  }

  "Creation with bucket size provider and directed flag" should "be possible" in{
    Given("Bucker size provider")
    val provider:BucketSizeProvider[Long,Long]=(g:Graph[Long,Long])=>1l
    When("Configuration creation")
    VertexMeasureConfiguration( false, provider)
  }
}

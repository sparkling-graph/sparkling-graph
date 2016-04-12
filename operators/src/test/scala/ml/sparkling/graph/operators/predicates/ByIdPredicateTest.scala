package ml.sparkling.graph.operators.predicates

import org.scalatest.{FlatSpec, GivenWhenThen}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class ByIdPredicateTest extends FlatSpec with GivenWhenThen {

  "True" should "be returned if correct id is given" in{
    Given("Vertex id and predicate")
    val vertexId=1l
    val predicate=ByIdPredicate(vertexId)
    When("Checked")
    val checkResult: Boolean = predicate.apply(vertexId)
    Then("should return true")
    assert(checkResult)
  }

  "False" should "be returned if incorrect id is given" in{
    Given("Vertex id and predicate")
    val vertexId=1l
    val predicate=ByIdPredicate(vertexId)
    When("Checked")
    val checkResult: Boolean = predicate.apply(2*vertexId)
    Then("should return false")
    assert(!checkResult)
  }

}

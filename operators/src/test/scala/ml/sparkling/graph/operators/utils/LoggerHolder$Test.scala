package ml.sparkling.graph.operators.utils

import org.apache.log4j.Logger
import org.scalatest.{FlatSpec, GivenWhenThen}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class LoggerHolder$Test extends FlatSpec with GivenWhenThen  {

  "Logger" should "be present in class" in{
    assert(LoggerHolder.log.isInstanceOf[Logger])
  }

}

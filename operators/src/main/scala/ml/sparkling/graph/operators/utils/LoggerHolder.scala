package ml.sparkling.graph.operators.utils

import org.apache.log4j.Logger

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 * Object used to hold logger and overcome spark serialization problems
 */
object LoggerHolder extends Serializable{
  @transient lazy val log = Logger.getLogger(getClass.getName)
}

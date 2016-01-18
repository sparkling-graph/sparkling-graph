package sparkling.graph.loaders.csv.providers

import org.apache.spark.sql.Row

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object PropertyProviders {

  def doubleAttributeProvider(index:Int=0)(row:Row): Double ={
    row.getAs[String](index).toDouble
  }

  def longAttributeProvider(index:Int=0)(row:Row): Long ={
    row.getAs[String](index).toLong
  }

}

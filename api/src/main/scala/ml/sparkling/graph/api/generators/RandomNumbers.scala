package ml.sparkling.graph.api.generators

import scala.util.Random

/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 27.04.16.
  */
object RandomNumbers{

 trait RandomNumberGenerator{
  def nextDouble():Double
  def nextLong(max:Long):Long
 }

 type RandomNumberGeneratorProvider=Long=>RandomNumberGenerator


 case class ScalaRandomNumberGenerator(val seed:Long) extends RandomNumberGenerator{
  lazy val generator=new Random(seed)
  override def nextDouble= generator.nextDouble
  override  def nextLong(max:Long)=(generator.nextDouble()*max).toLong
 }

 val ScalaRandomNumberGeneratorProvider=(seed:Long)=>ScalaRandomNumberGenerator(seed)


}

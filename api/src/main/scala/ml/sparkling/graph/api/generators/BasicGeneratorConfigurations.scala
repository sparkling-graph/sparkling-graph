package ml.sparkling.graph.api.generators

/**
  * Created by Roman Bartusiak riomus@gmail.com roman.bartusiak@pwr.edu.pl on 26.04.16.
  */
object BasicGeneratorConfigurations {
case class SingleParamConfig[PT](val param:PT) extends GraphGeneratorConfiguration
}

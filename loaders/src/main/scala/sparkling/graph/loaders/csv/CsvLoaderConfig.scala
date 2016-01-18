package sparkling.graph.loaders.csv

import org.apache.spark.sql.types.StructType

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
case class CsvLoaderConfig(header:Boolean=true,schema:StructType=null,delimiter:String=",",quote:String="\"",inferSchema:Boolean=false){

}


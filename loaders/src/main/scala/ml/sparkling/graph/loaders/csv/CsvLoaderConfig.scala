package ml.sparkling.graph.loaders.csv

import org.apache.spark.sql.types.StructType

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
case class CsvLoaderConfig(var header:Boolean=true,var schema:StructType=null,var delimiter:String=",", var quote:String="\"",var inferSchema:Boolean=false)

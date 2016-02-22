package ml.sparkling.graph.loaders.csv

import org.apache.spark.sql.DataFrameReader

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
object DataFrameReaderConfigurator {

  implicit class addAbilityToConfigureDataFrameReader(reader:DataFrameReader){
    def applyConfiguration(cSVLoaderConfig: CsvLoaderConfig):DataFrameReader={
      reader.option("header",cSVLoaderConfig.header.toString)
      reader.option("delimiter",cSVLoaderConfig.delimiter)
      reader.option("quote",cSVLoaderConfig.quote)
     cSVLoaderConfig.schema match{
       case None => reader.option("inferSchema",cSVLoaderConfig.inferSchema.toString)
       case Some(schema) => reader.schema(schema)
      }
      reader
    }
  }

}

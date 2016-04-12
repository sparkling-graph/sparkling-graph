import sbt._
import sbtsparkpackage.SparkPackagePlugin.autoImport.{spIncludeMaven, spName}

object SparkPackage extends AutoPlugin {


  override lazy val projectSettings = Seq(
    spIncludeMaven := true,
    spName:="sparkling-graph/sparkling-graph"
  )

  override def trigger = allRequirements


}
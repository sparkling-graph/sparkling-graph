
import sbt._
import sbt.Keys._
import java.io.File

object Publish extends AutoPlugin {


  override def trigger = allRequirements

  override lazy val projectSettings = Seq(
    crossPaths := false,
    publishTo := graphPublishTo.value,
    organizationName := "engine",
    organizationHomepage := Some(url("http://engine.pwr.wroc.pl/")),
    publishMavenStyle := true,
    pomIncludeRepository := { x => false }
  )

  private def graphPublishTo = Def.setting {
    localRepo()
  }

  private def localRepo() =
    Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

}
package sparkling.graph

import com.typesafe.sbt.SbtGhPages.ghpages
import com.typesafe.sbt.SbtSite.site
import sbt.Keys._
import sbt._
import sbtunidoc.Plugin.{ScalaUnidoc, unidocSettings}
import com.typesafe.sbt.SbtGit.GitKeys._
object SparklingGraphBuild extends Build {
  lazy val buildSettings = Dependencies.Versions ++ Seq(
    organization := "pl.edu.wroc.engine",
    version := "0.0.1-SNAPSHOT",
    parallelExecution in test := false,
    autoAPIMappings := true
  )

  val ghRef= sys.props.getOrElse("GH_REF", default = "git")
  val ghHost=sys.props.getOrElse("GH_HOST", default = "github.com:sparkling-graph/sparkling-graph.git")

  lazy val root = Project(id = "sparkling-graph",
    base = file("."), settings = buildSettings)
    .settings(unidocSettings: _*)
    .settings(site.settings ++ ghpages.settings: _*)
    .settings(
      site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "latest/api"),
      gitRemoteRepo := s"https://${ghRef}@${ghHost}"
    )
    .aggregate(api, loaders, operators, examples)

  lazy val loaders = Project(id = "sparkling-graph-loaders",
    base = file("loaders")).aggregate(api).dependsOn(api)

  lazy val operators = Project(id = "sparkling-graph-operators",
    base = file("operators")).aggregate(api).dependsOn(api)

  lazy val experiments = Project(id = "sparkling-graph-experiments",
    base = file("experiments")).aggregate(operators, api).dependsOn(operators % "provided->provided;compile->compile", api)

  lazy val api = Project(id = "sparkling-graph-api",
    base = file("api"))

  lazy val examples = Project(id = "sparkling-graph-examples",
    base = file("examples")).dependsOn(loaders % "provided->provided;compile->compile",
    operators % "provided->provided;compile->compile",experiments% "provided->provided;compile->compile")
}

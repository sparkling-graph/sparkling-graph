package sparkling.graph

import sbt.Keys._
import sbt._


object SparklingGraphBuild extends Build {
  lazy val buildSettings = Dependencies.Versions ++ Seq(
    organization := "pl.edu.wroc.engine",
    version := "0.0.1-SNAPSHOT",
    parallelExecution in test := false
  )

  lazy val root = Project(id = "sparkling-graph",
    base = file("."), settings = buildSettings) aggregate(api, loaders, operators, examples)

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

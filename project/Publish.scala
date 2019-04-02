package ml.sparkling.graph

import com.typesafe.sbt._
import sbt.Keys._
import sbt._
object Publish extends AutoPlugin {


  override lazy val projectSettings = Seq(
    SbtGit.GitKeys.useGitDescribe := true,
    licenses := Seq("BSD 2-Clause" -> url("http://opensource.org/licenses/BSD-2-Clause")),
    homepage := Some(url("https://github.com/sparkling-graph/sparkling-graph")),
    developers := List(Developer("riomus", "Roman Bartusiak", "riomus@gmail.com", url("https://bartusiak.ml"))),
    scmInfo := Some(ScmInfo(url("https://github.com/sparkling-graph/sparkling-graph"), "scm:git:git@github.com:sparkling-graph/sparkling-graph.git")),
  )

  override def trigger = allRequirements


}
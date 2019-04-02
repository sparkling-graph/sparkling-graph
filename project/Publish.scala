package ml.sparkling.graph

import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport._
import com.typesafe.sbt.SbtPgp.autoImportImpl._
import ch.epfl.scala.sbt.release.ReleaseEarlyPlugin.autoImport._
object Publish extends AutoPlugin {


  override lazy val projectSettings = Seq(
    releaseCrossBuild:= true,
    publishMavenStyle := true,
    pomIncludeRepository := { _ => false },
    useGpg := false,
    releaseEarlyWith in Global := SonatypePublisher,
    pgpPublicRing := file("./travis/local.pubring.asc"),
    pgpSecretRing := file("./travis/local.secring.asc"),
    licenses := Seq("BSD 2-Clause" -> url("http://opensource.org/licenses/BSD-2-Clause")),
    homepage := Some(url("https://github.com/sparkling-graph/sparkling-graph")),
    developers := List(Developer("riomus", "Roman Bartusiak", "$riomus@gmail.com", url("https://bartusiak.ml"))),
    scmInfo := Some(ScmInfo(url("https://github.com/sparkling-graph/sparkling-graph"), "scm:git:git@github.com:sparkling-graph/sparkling-graph.git")),

  )

  override def trigger = allRequirements


}
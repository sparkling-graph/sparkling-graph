package ml.sparkling.graph

import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport._
import com.typesafe.sbt.SbtPgp.autoImportImpl._
object Publish extends AutoPlugin {


  override lazy val projectSettings = Seq(
    releaseCrossBuild:= true,
    publishMavenStyle := true,
    pomIncludeRepository := { _ => false },
    useGpg := false,
    pgpPublicRing := file("./travis/local.pubring.asc")
    pgpSecretRing := file("./travis/local.secring.asc")
    pomExtra := (
      <url>https://sparkling.ml</url>
        <scm>
          <url>git@github.com:sparkling-graph/sparkling-graph.git</url>
          <connection>scm:git:git@github.com:sparkling-graph/sparkling-graph.git</connection>
        </scm>
        <developers>
          <developer>
            <id>riomus</id>
            <name>Roman Bartusiak</name>
            <url>http://bartusiak.ml</url>
          </developer>
        </developers>)
  )

  override def trigger = allRequirements


}
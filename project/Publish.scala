package ml.sparkling.graph

import sbt.Keys._
import sbt._

object Publish extends AutoPlugin {


  override lazy val projectSettings = Seq(
    publishMavenStyle := true,
    pomIncludeRepository := { _ => false },
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
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
            <url>http://www.riomus.ml</url>
          </developer>
        </developers>)
  )

  override def trigger = allRequirements


}
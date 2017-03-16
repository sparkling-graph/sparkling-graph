package ml.sparkling.graph

import com.typesafe.sbt.SbtGhPages.GhPagesKeys._
import com.typesafe.sbt.SbtGhPages.ghpages
import com.typesafe.sbt.SbtGit.GitKeys
import com.typesafe.sbt.SbtGit.GitKeys._
import com.typesafe.sbt.SbtSite.site
import com.typesafe.sbt.git.GitRunner
import com.typesafe.sbt.pgp.PgpKeys
import sbt.Keys._
import sbt.Scoped.RichTaskable3
import sbt._
import sbtrelease.ReleasePlugin.autoImport.releasePublishArtifactsAction
import sbtunidoc.Plugin.{ScalaUnidoc, unidocSettings}

object SparklingGraphBuild extends Build {
  lazy val buildSettings = Dependencies.Versions ++ Seq(
    organization := "ml.sparkling",
    autoAPIMappings := true,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    licenses += ("GNU General Public License 3.0", url("http://www.gnu.org/licenses/gpl-3.0.en.html"))
  )
  lazy val root = Project(id = "sparkling-graph",
    base = file("."), settings = buildSettings)
    .settings(unidocSettings: _*)
    .settings(site.settings ++ ghpages.settings: _*)
    .settings(
      site.addMappingsToSiteDir(mappings in(ScalaUnidoc, packageDoc), "latest/api"),
      gitRemoteRepo := ghRepo,
      pushSite <<= pushSite0
    )
    .aggregate(api, loaders, operators,generators, examples,experiments,utils)
  lazy val loaders = Project(id = "sparkling-graph-loaders",
    base = file("loaders")).aggregate(api).dependsOn(api)
  lazy val generators = Project(id = "sparkling-graph-generators",
    base = file("generators")).aggregate(api).dependsOn(api)
  lazy val utils = Project(id = "sparkling-graph-utils",
    base = file("utils")).aggregate(api).dependsOn(api)
  lazy val operators = Project(id = "sparkling-graph-operators",
    base = file("operators")).aggregate(api).dependsOn(api).dependsOn(loaders)
  lazy val experiments = Project(id = "sparkling-graph-experiments",
    base = file("experiments")).aggregate(operators, api).dependsOn(operators % "provided->provided;compile->compile", api)
  lazy val api = Project(id = "sparkling-graph-api",
    base = file("api"))
  lazy val examples = Project(id = "sparkling-graph-examples",
    base = file("examples")).dependsOn(loaders % "provided->provided;compile->compile",
    operators % "provided->provided;compile->compile", experiments % "provided->provided;compile->compile",generators% "provided->provided;compile->compile")
  val ghToken = sys.env.getOrElse("GH_TOKEN", default = "git")
  val ghHost = sys.env.getOrElse("GH_HOST", default = "github.com/sparkling-graph/sparkling-graph.git")
  val ghRepo = s"https://${ghToken}@${ghHost}"
  val commitMessage = sys.env.getOrElse("SBT_GHPAGES_COMMIT_MESSAGE", "[ci skip] updated site")
  parallelExecution in ThisBuild := false
  private def pushSite0: RichTaskable3[File, GitRunner, TaskStreams]#App[Unit] = (synchLocal, GitKeys.gitRunner, streams) map { (repo, git, s) =>
    git("add", ".")(repo, s.log)
    git("commit", "-m", commitMessage, "--allow-empty")(repo, s.log)
    git("push", "--force", "--quiet", s"${ghRepo}")(repo, s.log)
  }
}

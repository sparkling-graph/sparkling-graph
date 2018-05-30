//package ml.sparkling.graph
//
//import sbt._
//import Keys._
//import com.typesafe.sbt.pgp.PgpKeys
//
//object SparklingGraphBuild extends Build {
//  lazy val buildSettings = Dependencies.Versions ++ Seq(
//    organization := "ml.sparkling",
//    autoAPIMappings := true,
////    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
//    licenses += ("BSD 2-clause \"Simplified\" License", url("https://opensource.org/licenses/BSD-2-Clause"))
//  )
//  lazy val root = Project(id = "sparkling-graph",
//    base = file(".")).settings(buildSettings)
////    .enablePlugins(GhpagesPlugin, ScalaUnidocPlugin)
////    .settings(unidocSettings: _*)
////    .settings(site.settings ++ ghpages.settings: _*)
//    .settings(
////      site.addMappingsToSiteDir(mappings in(ScalaUnidoc, packageDoc), "latest/api"),
////      gitRemoteRepo := ghRepo,
////      pushSite <<= pushSite0
//    )
//    .aggregate(api, loaders, operators,generators, examples,experiments,utils)
//  lazy val loaders = Project(id = "sparkling-graph-loaders",
//    base = file("loaders")).aggregate(api).dependsOn(api)
//  lazy val generators = Project(id = "sparkling-graph-generators",
//    base = file("generators")).aggregate(api).dependsOn(api)
//  lazy val utils = Project(id = "sparkling-graph-utils",
//    base = file("utils")).aggregate(api).dependsOn(api)
//  lazy val operators = Project(id = "sparkling-graph-operators",
//    base = file("operators")).aggregate(api).dependsOn(api).dependsOn(loaders).dependsOn(generators%"compile->test")
//  lazy val experiments = Project(id = "sparkling-graph-experiments",
//    base = file("experiments")).aggregate(operators, api).dependsOn(operators % "provided->provided;compile->compile", api)
//  lazy val api = Project(id = "sparkling-graph-api",
//    base = file("api"))
//  lazy val examples = Project(id = "sparkling-graph-examples",
//    base = file("examples")).dependsOn(loaders % "provided->provided;compile->compile",
//    operators % "provided->provided;compile->compile", experiments % "provided->provided;compile->compile",generators% "provided->provided;compile->compile")
//  val ghToken = sys.env.getOrElse("GH_TOKEN", default = "git")
//  val ghHost = sys.env.getOrElse("GH_HOST", default = "github.com/sparkling-graph/sparkling-graph.git")
//  val ghRepo = s"https://${ghToken}@${ghHost}"
//  val commitMessage = sys.env.getOrElse("SBT_GHPAGES_COMMIT_MESSAGE", "[ci skip] updated site")
//  parallelExecution in ThisBuild := true
////  private def pushSite0: RichTaskable3[File, GitRunner, TaskStreams]#App[Unit] = (synchLocal, GitKeys.gitRunner, streams) map { (repo, git, s) =>
////    git("add", ".")(repo, s.log)
////    git("commit", "-m", commitMessage, "--allow-empty")(repo, s.log)
////    git("push", "--force", "--quiet", s"${ghRepo}")(repo, s.log)
////  }
//}

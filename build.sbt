import com.typesafe.sbt.SbtGit.GitKeys
import ml.sparkling.graph.Dependencies
val ghToken = sys.env.getOrElse("GH_TOKEN", default = "git")
val ghHost = sys.env.getOrElse("GH_HOST", default = "github.com/sparkling-graph/sparkling-graph.git")
val ghRepo = s"https://${ghToken}@${ghHost}"
parallelExecution in ThisBuild := true

lazy val buildSettings = Dependencies.Versions ++ Seq(
  organization := "ml.sparkling",
  autoAPIMappings := true
)
lazy val root = Project(id = "sparkling-graph",
  base = file(".")).settings(buildSettings)
      .enablePlugins(GhpagesPlugin, ScalaUnidocPlugin, SiteScaladocPlugin)
  .settings(
        siteSubdirName in ScalaUnidoc := "latest/api",
        addMappingsToSiteDir(mappings in(ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc),
        git.remoteRepo := ghRepo,
          publishTo := sonatypePublishTo.value
)
  .aggregate(api, loaders, operators,generators, examples,experiments,utils)
lazy val loaders = Project(id = "sparkling-graph-loaders",
  base = file("loaders")).settings(buildSettings).aggregate(api).dependsOn(api)
lazy val generators = Project(id = "sparkling-graph-generators",
  base = file("generators")).settings(buildSettings).aggregate(api).dependsOn(api)
lazy val utils = Project(id = "sparkling-graph-utils",
  base = file("utils")).settings(buildSettings).aggregate(api).dependsOn(api)
lazy val operators = Project(id = "sparkling-graph-operators",
  base = file("operators")).settings(buildSettings).aggregate(api).dependsOn(api).dependsOn(loaders).dependsOn(generators%"compile->test")
lazy val experiments = Project(id = "sparkling-graph-experiments",
  base = file("experiments")).settings(buildSettings).aggregate(operators, api).dependsOn(operators % "provided->provided;compile->compile", api)
lazy val api = Project(id = "sparkling-graph-api",
  base = file("api")).settings(buildSettings)
lazy val examples = Project(id = "sparkling-graph-examples",
  base = file("examples")).settings(buildSettings).dependsOn(loaders % "provided->provided;compile->compile",
  operators % "provided->provided;compile->compile", experiments % "provided->provided;compile->compile",generators% "provided->provided;compile->compile")


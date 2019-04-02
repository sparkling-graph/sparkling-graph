addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.2.7")
addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.1")
resolvers += "jgit-repo" at "http://download.eclipse.org/jgit/maven"
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.2")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.8")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")
resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"
resolvers += Resolver.bintrayIvyRepo("s22s", "sbt-plugins")
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.7-astraea.1")
addSbtPlugin("ch.epfl.scala" % "sbt-release-early" % "2.1.1")
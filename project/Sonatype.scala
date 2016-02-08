import sbt.Keys._
import sbt._

object Sonatype extends AutoPlugin {


  val username = Option(System.getenv().get("SONATYPE_USERNAME"))
  val password = Option(System.getenv().get("SONATYPE_PASSWORD"))

  override def trigger = allRequirements


  if (username.isDefined && password.isDefined) {
    credentials += Credentials(
      "Sonatype Nexus Repository Manager",
      "oss.sonatype.org",
      username.get,
      password.get)

  } else {
    credentials ++= Seq()
  }


}
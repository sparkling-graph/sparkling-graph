import sbt.Keys._
import sbt._

object Sonatype extends AutoPlugin {


  val username = Option(System.getenv().get("SONATYPE_USERNAME"))
  val password = Option(System.getenv().get("SONATYPE_PASSWORD"))

  override def trigger = allRequirements

  val toAdd=(username, password) match {
    case (Some(u), Some(p)) => Seq(Credentials( "Sonatype Nexus Repository Manager","oss.sonatype.org", u, p))
    case (_, _) => Seq()
  }

  credentials++=toAdd

}
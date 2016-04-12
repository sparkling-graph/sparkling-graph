val username = Option(System.getenv().get("SONATYPE_USERNAME"))
val password = Option(System.getenv().get("SONATYPE_PASSWORD"))
(username, password) match {
  case (Some(u), Some(p)) => {
    println("Loading credentials from env"); credentials += Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", u, p)
  }
  case (_, _) => credentials ++= Seq()
}

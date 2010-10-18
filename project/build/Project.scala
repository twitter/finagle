import sbt._

class Project(info: ProjectInfo) extends DefaultProject(info) {
  override def managedStyle = ManagedStyle.Maven

  val nettyRepo = "repository.jboss.org" at "http://repository.jboss.org/nexus/content/groups/public/"

  val netty = "org.jboss.netty" % "netty" % "3.2.2.Final" withSources()
  val util = "com.twitter" %% "util" % "1.2-SNAPSHOT"

  val mockito = "org.mockito" % "mockito-all" % "1.8.5" % "test" withSources()
  val specs = "org.scala-tools.testing" %% "specs" % "1.6.5" % "test" withSources()
}

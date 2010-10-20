import sbt._
import com.twitter.sbt._

class Project(info: ProjectInfo) extends StandardProject(info) {
  override def disableCrossPaths = false

  override def managedStyle = ManagedStyle.Maven

  val nettyRepo = "repository.jboss.org" at "http://repository.jboss.org/nexus/content/groups/public/"

  val netty = "org.jboss.netty" % "netty" % "3.2.2.Final" withSources()
  val util = "com.twitter" %% "util" % "1.2-SNAPSHOT"

  val mockito = "org.mockito" % "mockito-all" % "1.8.5" % "test" withSources()
  val specs = "org.scala-tools.testing" %% "specs" % "1.6.5" % "test" withSources()

  override def compileThriftAction(lang: String) = task {
    import Process._
    outputPath.asFile.mkdirs()
    val tasks = thriftSources.getPaths.map { path =>
      val thriftBinary = System.getProperty("THRIFT_BINARY", "thrift")
      execTask { "%s --gen %s -o %s %s".format(thriftBinary, lang, outputPath.absolutePath, path) }
    }
    if (tasks.isEmpty) None else tasks.reduceLeft { _ && _ }.run
  }

  // Temporary?
  val thrift = "thrift" % "libthrift" % "0.2.0"
  val slf4j = "org.slf4j" % "slf4j-simple" % "1.5.8"
  val slf4jApi = "org.slf4j" % "slf4j-api" % "1.5.8"
}

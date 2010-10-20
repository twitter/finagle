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
      execTask { "/Users/marius/pkg/thrift-0.5.0/compiler/cpp/thrift --gen %s -o %s %s".format(lang, outputPath.absolutePath, path) }
    }
    if (tasks.isEmpty) None else tasks.reduceLeft { _ && _ }.run
  }


  // Temporary?
  val thrift = "org.apache.thrift" % "libthrift" % "0.5.0"
}

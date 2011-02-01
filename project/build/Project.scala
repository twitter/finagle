import sbt._
import com.twitter.sbt._

class Project(info: ProjectInfo) extends StandardParentProject(info)
  with SubversionPublisher
{
  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public")

  val twitterRepo  = "twitter.com" at "http://maven.twttr.com/"

  /**
   * finagle-core contains the finagle kernel itself, plus builders,
   * HTTP codecs [HTTP may move to its own project soon]
   */
  val coreProject = project(
    "finagle-core", "finagle-core",
    new CoreProject(_))

  /**
   * finagle-ostrich implements a StatsReceiver for the Ostrich statistics library
   */
  val ostrichProject = project(
    "finagle-ostrich", "finagle-ostrich",
    new OstrichProject(_), coreProject)

  /**
   * finagle-thrift contains thrift codecs for use with the finagle
   * thrift service codegen.
   */
  val thriftProject = project(
    "finagle-thrift", "finagle-thrift",
    new ThriftProject(_), coreProject)

  /**
   * finagle-memcached contains the memcached codec, ketama, and Java and Scala
   * friendly clients.
   */
  val memcachedProject = project(
    "finagle-memcached", "finagle-memcached",
    new MemcachedProject(_), coreProject)

  /**
   * finagle-kestrel contains the kestrel codec and Java and Scala
   * friendly clients.
   */
  val kestrelProject = project(
    "finagle-kestrel", "finagle-kestrel",
    new KestrelProject(_), coreProject, memcachedProject)

  /**
   * finagle-stress has stress/integration test suites & tools for
   * development.
   */
  val stressProject = project(
    "finagle-stress", "finagle-stress",
    new StressProject(_), coreProject, ostrichProject)

  class CoreProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    override def compileOrder = CompileOrder.ScalaThenJava

    val nettyRepo =
      "repository.jboss.org" at "http://repository.jboss.org/nexus/content/groups/public/"
    val netty     = "org.jboss.netty"      %  "netty"     % "3.2.3.Final"
    val util      = "com.twitter"          %  "util"      % "1.5.2"

    val mockito   = "org.mockito"             % "mockito-all" % "1.8.5" % "test" withSources()
    val specs     = "org.scala-tools.testing" % "specs_2.8.0" % "1.6.5" % "test" withSources()
  }

  class ThriftProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with LibDirClasspath with AdhocInlines
  {
    override def compileOrder = CompileOrder.JavaThenScala
    val thrift   = "thrift"    %  "libthrift" % "0.5.0"
    val slf4jNop = "org.slf4j" %  "slf4j-nop" % "1.5.2" % "provided"
  }

  class MemcachedProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    override def compileOrder = CompileOrder.ScalaThenJava
    val junit = "junit" % "junit" % "3.8.2" % "test"
  }

  class KestrelProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    override def compileOrder = CompileOrder.ScalaThenJava
  }

  class OstrichProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    val ostrich = "com.twitter" % "ostrich" % "2.3.4"
  }

  class StressProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with IntegrationSpecs with AdhocInlines
  {
    val ostrich = "com.twitter" % "ostrich" % "2.3.4"
  }
}

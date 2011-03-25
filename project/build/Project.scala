import sbt._
import com.twitter.sbt._

class Project(info: ProjectInfo) extends StandardParentProject(info)
  with SubversionPublisher
  with AdhocInlines
{
  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public")

  val twitterRepo = "twitter.com" at "http://maven.twttr.com/"

  override def ivyXML =
    <dependencies>
      <exclude module="jms"/>
      <exclude module="jmxtools"/>
      <exclude module="jmxri"/>
    </dependencies>

  /**
   * finagle-core contains the finagle kernel itself, plus builders,
   * HTTP codecs [HTTP may move to its own project soon]
   */
  val coreProject = project(
    "finagle-core", "finagle-core",
    new CoreProject(_))

  /**
   * finagle-ostrich implements a StatsReceiver for the Ostrich 2.x statistics library
   */
  val ostrichProject = project(
    "finagle-ostrich", "finagle-ostrich",
    new OstrichProject(_), coreProject)

  /**
   * finagle-ostrich3 implements a StatsReceiver for the Ostrich 3.x statistics library
   */
  val ostrich3Project = project(
    "finagle-ostrich3", "finagle-ostrich3",
    new Ostrich3Project(_), coreProject)

  /**
   * finagle-ostrich4 implements a StatsReceiver for the Ostrich 4.x statistics library
   */
  val ostrich4Project = project(
    "finagle-ostrich4", "finagle-ostrich4",
    new Ostrich4Project(_), coreProject)

  /**
   * finagle-thrift contains thrift codecs for use with the finagle
   * thrift service codegen. in order to be able to use thrift code
   * generation in finagle-thrift.
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
   * finagle-native contains native code aimed to increase platform fluency
   * and provide capabilities not available in the JVM
   */
  val nativeProject = project(
    "finagle-native", "finagle-native",
    new NativeProject(_), coreProject)

  /**
   * finagle-stream contains a streaming http codec identical to
   * Twitter's "firehose".
   */
  val streamProject = project(
    "finagle-stream", "finagle-stream",
    new StreamProject(_), coreProject, kestrelProject)

  /**
   * finagle-serversets contains a cluster implementation using
   * twitter-common's Zookeeper-backed serverset implementation.
   */
  val serversetsProject = project(
    "finagle-serversets", "finagle-serversets",
    new ServersetsProject(_), coreProject)

  /**
   * finagle-stream contains a streaming http codec identical to
   * Twitter's "firehose".
   */
  val exampleProject = project(
    "finagle-example", "finagle-example",
    new ExampleProject(_),
    coreProject, streamProject, thriftProject,
    memcachedProject, kestrelProject)

  /**
   * finagle-stress has stress/integration test suites & tools for
   * development.
   */
  val stressProject = project(
    "finagle-stress", "finagle-stress",
    new StressProject(_), coreProject, ostrich4Project, thriftProject)

  class CoreProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    override def compileOrder = CompileOrder.ScalaThenJava

    val nettyRepo =
      "repository.jboss.org" at "http://repository.jboss.org/nexus/content/groups/public/"
    val netty           = "org.jboss.netty" %  "netty"           % "3.2.3.Final"
    val utilCore        = "com.twitter"     %  "util-core"       % "1.8.5" relativePath("util")
    val utilCollection  = "com.twitter"     %  "util-collection" % "1.8.5" relativePath("util")

    // Testing:
    val mockito = "org.mockito"             % "mockito-all"      % "1.8.5" % "test" withSources()
    val specs   = "org.scala-tools.testing" % "specs_2.8.0"      % "1.6.5" % "test" withSources()
  }

  class ThriftProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with LibDirClasspath
    with AdhocInlines with CompileThriftFinagle
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

  class StreamProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    override def compileOrder = CompileOrder.ScalaThenJava
  }

  class ServersetsProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    override def ivyXML =
      <dependencies>
        <exclude module="jms"/>
        <exclude module="jmxtools"/>
        <exclude module="jmxri"/>
      </dependencies>

    val commonsZookeeper = "com.twitter.common" % "zookeeper" % "0.0.14"
  }

  class ExampleProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines with CompileThriftFinagle


  class OstrichProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    val ostrich2 = "com.twitter" % "ostrich" % "2.3.4"
  }

  class Ostrich3Project(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    val ostrich3 = "com.twitter" % "ostrich" % "3.0.4"
  }

  class Ostrich4Project(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    val ostrich4 = "com.twitter" % "ostrich" % "4.0.2"
  }

  class NativeProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines with LibDirClasspath

  class StressProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with IntegrationSpecs with AdhocInlines
    with CompileThriftFinagle
  {
    override def compileOrder = CompileOrder.JavaThenScala
    val thrift   = "thrift"    %  "libthrift" % "0.5.0"
    val slf4jNop = "org.slf4j" %  "slf4j-nop" % "1.5.2" % "provided"
    val ostrich3 = "com.twitter" % "ostrich" % "3.0.4"
  }
}

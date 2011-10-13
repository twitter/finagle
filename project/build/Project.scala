import sbt._
import com.twitter.sbt._

class Project(info: ProjectInfo) extends StandardParentProject(info)
  with SubversionPublisher
  with ParentProjectDependencies
  with DefaultRepos
{
  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public")

  val nettyRepo =
    "repository.jboss.org" at "http://repository.jboss.org/nexus/content/groups/public/"

  val reflectionsRepo =
    "reflections.googlecode.com" at "http://reflections.googlecode.com/svn/repo"

  val codahaleRepo =
    "repo.codahale.com" at "http://repo.codahale.com"

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
   * finagle-exception implements an ExceptionReceiver for the yet-to-be-named
   * (if at all) exception service.
   */
  val exceptionProject = project(
    "finagle-exception", "finagle-exception",
    new ExceptionProject(_), coreProject, thriftProject)

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
   * finagle-http contains an http codec.
   */
  val httpProject = project(
    "finagle-http", "finagle-http",
    new HttpProject(_), coreProject)

  /**
   * finagle-native contains native code aimed to increase platform fluency
   * and provide capabilities not available in the JVM
   */
  val nativeProject = project(
    "finagle-native", "finagle-native",
    new NativeProject(_), coreProject, httpProject)

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
    coreProject, httpProject, streamProject, thriftProject,
    memcachedProject, kestrelProject)

  /**
   * finagle-stress has stress/integration test suites & tools for
   * development.
   */
  val stressProject = project(
    "finagle-stress", "finagle-stress",
    new StressProject(_), coreProject, ostrich4Project, thriftProject, httpProject)

  /**
   * finagle-b3 contains bindings for the B3, or BigBrotherBird, tracing
   * framework. Send messages via scribe that is collected, indexed and analyzed.
   */
  val b3Project = project(
    "finagle-b3", "finagle-b3",
    new B3Project(_), coreProject, thriftProject)

  /**
   * finagle-commons-stats contains bindings for using finagle in java projects
   * that export their stats through the twitter commons libraries
   */
  val commonsStatsProject = project(
    "finagle-commons-stats", "finagle-commons-stats",
    new CommonsStatsProject(_), coreProject)

  trait Defaults
    extends ProjectDependencies
    with DefaultRepos
    with SubversionPublisher

  class CoreProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def compileOrder = CompileOrder.ScalaThenJava
    val netty = "org.jboss.netty" %  "netty" % "3.2.5.Final"

    projectDependencies(
      "util" ~ "util-core",
      "util" ~ "util-collection",
      "util" ~ "util-hashing"
    )

    // Testing:
    val mockito = "org.mockito"             % "mockito-all"      % "1.8.5" % "test" withSources()
    val specs   = "org.scala-tools.testing" %% "specs"      % "1.6.8" % "test" withSources()
  }

  class ThriftProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults with LibDirClasspath with CompileThriftFinagle
  {
    // Some of the autogenerated java code cause javadoc errors.
    override def docSources = sources(mainScalaSourcePath##)

    override def compileOrder = CompileOrder.JavaThenScala
    val thrift    = "thrift"    % "libthrift" % "0.5.0"
    val slf4jNop  = "org.slf4j" % "slf4j-nop" % "1.5.8" % "provided"
  }

  class MemcachedProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def compileOrder = CompileOrder.ScalaThenJava
    val junit = "junit" % "junit" % "3.8.2" % "test"

    projectDependencies(
      "util" ~ "util-hashing"
    )
  }

  class KestrelProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def compileOrder = CompileOrder.ScalaThenJava
  }

  class HttpProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def compileOrder = CompileOrder.ScalaThenJava

    projectDependencies(
      "util" ~ "util-codec",
      "util" ~ "util-logging"
    )

    val commonsLang = "commons-lang" % "commons-lang" % "2.6" // for FastDateFormat
  }

  class StreamProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def compileOrder = CompileOrder.ScalaThenJava
  }

  class ServersetsProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def ivyXML =
      <dependencies>
        <exclude module="jms"/>
        <exclude module="jmxtools"/>
        <exclude module="jmxri"/>
        <exclude module="google-collections"/> // is subset of guava, which is also included
        <override org="commons-codec" rev="1.5"/>
      </dependencies>

    val commonsZookeeper = "com.twitter.common" % "zookeeper" % "0.0.24"
  }

  class ExampleProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults with CompileThriftFinagle
  {
    val slf4jNop = "org.slf4j" %  "slf4j-nop" % "1.5.8" % "provided"

    projectDependencies(
      "util" ~ "util-codec"
    )
  }

  class OstrichProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    val ostrich2 = "com.twitter" % "ostrich" % "2.3.4"
  }

  class Ostrich4Project(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    projectDependencies("ostrich")
  }

  class NativeProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults with LibDirClasspath

  class StressProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults with IntegrationSpecs with CompileThriftFinagle
  {
    override def compileOrder = CompileOrder.JavaThenScala
    val thrift   = "thrift"      % "libthrift" % "0.5.0"
    val slf4jNop = "org.slf4j"   % "slf4j-nop" % "1.5.8" % "provided"
    projectDependencies("ostrich")
  }

  class B3Project(info: ProjectInfo) extends StandardProject(info)
    with Defaults with LibDirClasspath with CompileThriftFinagle
  {
    override def compileOrder = CompileOrder.JavaThenScala
    val thrift    = "thrift"    % "libthrift" % "0.5.0"
    val slf4jNop  = "org.slf4j" % "slf4j-nop" % "1.5.8" % "provided"

    projectDependencies(
      "util" ~ "util-codec"
    )
  }

  class ExceptionProject(info: ProjectInfo) extends StandardProject(info)
   with Defaults with CompileThriftFinagle
  {
    val codaRepo            = "Coda Hale's Repository" at "http://repo.codahale.com/"
    override def compileOrder = CompileOrder.JavaThenScala

    val thrift    = "thrift"    % "libthrift" % "0.5.0"
    val jerkson  = "com.codahale" % "jerkson_2.8.1" % "0.1.4"
    val jacksonCore = "org.codehaus.jackson" % "jackson-core-asl"  % "1.8.1"
    val jacksonMapper = "org.codehaus.jackson" % "jackson-mapper-asl" % "1.8.1"
    val slf4jNop  = "org.slf4j" % "slf4j-nop" % "1.5.8" % "provided"

    // Needed for spec testing only
    val streamy = "com.twitter" % "streamyj_2.8.1" % "0.3.0"

    projectDependencies(
      "util" ~ "util-codec"
    )
  }

  class CommonsStatsProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults with LibDirClasspath
  {
    override def compileOrder = CompileOrder.JavaThenScala
    val commonsStats    = "com.twitter.common"    % "stats" % "0.0.16"
  }
}

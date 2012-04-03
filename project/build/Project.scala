

import com.twitter.sbt._
import sbt._

class Project(info: ProjectInfo) extends StandardParentProject(info)
  with SubversionPublisher
  with ParentProjectDependencies
  with DefaultRepos
{
  override def usesMavenStyleBasePatternInPublishLocalConfiguration = true
  override def subversionRepository = Some("https://svn.twitter.biz/maven-public")

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
   * finagle-test contains shared test utilities
   */
  val finagleTestProject = project(
    "finagle-test", "finagle-test",
    new TestProject(_))

  /**
   * finagle-core contains the finagle kernel itself, plus builders,
   * HTTP codecs [HTTP may move to its own project soon]
   */
  val coreProject = project(
    "finagle-core", "finagle-core",
    new CoreProject(_), finagleTestProject)


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
    new ThriftProject(_), coreProject, finagleTestProject)

  /**
   * Codec for protobuf RPC. Disabled by default until we've
   * settled on a protocol.
   */
/*
  val protobufProject = project(
    "finagle-protobuf", "finagle-protobuf",
    new ProtobufProject(_), coreProject)
*/

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
   * finagle-memcached contains a memcached reducer
   */
  val memcachedHadoopProject = project(
    "finagle-memcached-hadoop", "finagle-memcached-hadoop",
    new MemcachedHadoopProject(_), coreProject, memcachedProject)

  /**
   * finagle-kestrel contains the kestrel codec and Java and Scala
   * friendly clients.
   */
  val kestrelProject = project(
    "finagle-kestrel", "finagle-kestrel",
    new KestrelProject(_), coreProject, memcachedProject)

  /**
   * finagle-redis is a redis codec contributed by Tumblr.
   */
  val redisProject = project(
    "finagle-redis", "finagle-redis",
    new RedisProject(_), coreProject)

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
   * Examples for finagle
   */
  val exampleProject = project(
    "finagle-example", "finagle-example",
    new ExampleProject(_),
    coreProject, httpProject, streamProject, thriftProject,
    memcachedProject, kestrelProject, redisProject, ostrich4Project)

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
    val netty = "io.netty" % "netty" % "3.4.0.Alpha2" withSources()

    projectDependencies(
      "util" ~ "util-core",
      "util" ~ "util-collection",
      "util" ~ "util-hashing"
    )

    // Testing:
    val mockito = "org.mockito"             %  "mockito-all" % "1.8.5" % "test" withSources()
    val specs   = "org.scala-tools.testing" %% "specs"      % "1.6.8" % "test" withSources()
    val junit   = "junit"                   %  "junit"      % "4.8.1" % "test" withSources()
  }

  class TestProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def compileOrder = CompileOrder.ScalaThenJava
    val netty = "io.netty" % "netty" % "3.4.0.Alpha1" withSources()
    projectDependencies(
      "util" ~ "util-core"
    )
  }

  class ThriftProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults with LibDirClasspath with CompileThriftFinagle
  {
    // Some of the autogenerated java code cause javadoc errors.
    override def docSources = sources(mainScalaSourcePath##)
    override def dependencyPath = "lib"

    override def compileOrder = CompileOrder.JavaThenScala
    val thrift    = "org.apache.thrift" % "libthrift" % "0.5.0" intransitive()
    val sillyThrift = "silly" % "silly-thrift" % "0.5.0"
    val slf4jNop  = "org.slf4j" % "slf4j-nop" % "1.5.8" % "provided"
  }

  class MemcachedProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def compileOrder = CompileOrder.ScalaThenJava
    val junit = "junit" % "junit" % "4.8.1" % "test"

    projectDependencies(
      "util" ~ "util-hashing"
    )
  }

  class MemcachedHadoopProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def compileOrder = CompileOrder.ScalaThenJava
    val junit = "junit" % "junit" % "4.8.1" % "test"

    projectDependencies(
      "util" ~ "util-eval"
    )

    val hadoop    = "org.apache.hadoop" % "hadoop-core" % "0.20.2"
    val codec     = "commons-codec" % "commons-codec" % "1.5"
    val pig       = "org.apache.pig" % "pig" % "0.9.2"
    val mrunit    = "org.apache.mrunit" % "mrunit" % "0.8.0-incubating" % "test"
  }

  class KestrelProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def compileOrder = CompileOrder.ScalaThenJava
  }

  class ProtobufProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    override def compileOrder = CompileOrder.ScalaThenJava

    val protobuf    = "com.google.protobuf" % "protobuf-java" % "2.4.1"
    val slf4jNop  = "org.slf4j" % "slf4j-nop" % "1.5.8" % "provided"
    val junit = "junit" % "junit" % "4.10"
  }

  class RedisProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults
  {
    val naggati = "com.twitter" % "naggati" % "2.2.0" intransitive()

    // This is currently disabled since it requires the user to have a redis
    // installation. We might be able to ship the redis binary with some
    // architectures, and make the test conditional.
    override def testOptions = {
      val tests = List(
        "com.twitter.finagle.redis.protocol.integration.ClientServerIntegrationSpec",
        "com.twitter.finagle.redis.integration.ClientSpec")
      ExcludeTests(tests) :: super.testOptions.toList
    }

    projectDependencies(
      "util" ~ "util-logging"
    )
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
    val commonsCodec    = "commons-codec" % "commons-codec" % "1.5"
    val commonClient    = "com.twitter.common.zookeeper" % "client"     % "0.0.6"
    val commonGroup     = "com.twitter.common.zookeeper" % "group"      % "0.0.5"
    val commonServerSet = "com.twitter.common.zookeeper" % "server-set" % "0.0.5"
  }

  class ExampleProject(info: ProjectInfo) extends StandardProject(info)
    with Defaults with CompileThriftFinagle
  {
    val slf4jNop = "org.slf4j" %  "slf4j-nop" % "1.5.8" % "provided"
    val commonServerSet = "com.twitter.common" % "flags" % "0.0.1"

    projectDependencies(
      "util" ~ "util-codec"
    )
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
    val thrift   = "org.apache.thrift" % "libthrift" % "0.5.0" intransitive()
    val slf4jNop = "org.slf4j"   % "slf4j-nop" % "1.5.8" % "provided"
    projectDependencies(
      "ostrich",
      "util" ~ "util-logging"
    )
  }

  class B3Project(info: ProjectInfo) extends StandardProject(info)
    with Defaults with LibDirClasspath with CompileThriftFinagle
  {
    override def compileOrder = CompileOrder.JavaThenScala
    val thrift    = "org.apache.thrift" % "libthrift" % "0.5.0" intransitive()
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

    val thrift   = "org.apache.thrift" % "libthrift" % "0.5.0" intransitive()
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

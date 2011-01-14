import sbt._
import com.twitter.sbt._

class Project(info: ProjectInfo) extends StandardParentProject(info)
  with SubversionPublisher
{
//  override def parallelExecution = true
  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public")

  val twitterRepo  = "twitter.com" at "http://maven.twttr.com/"

  // finagle-core contains the finagle kernel itself, plus builders &
  // HTTP codecs [HTTP may move to its own project soon]
  val coreProject = project(
    "finagle-core", "finagle-core",
    new CoreProject(_))

  // finagle-ostrich has a StatsReceiver for Ostrich
  val ostrichProject = project(
    "finagle-ostrich", "finagle-ostrich",
    new OstrichProject(_), coreProject)

  // finagle-thrift contains thrift codecs
  //   DISABLE until new thrift.  too messy with new types.
  // val thriftProject = project(
  //   "finagle-thrift", "finagle-thrift",
  //   new ThriftProject(_), coreProject)

  // finagle-stress has stress/integration test suites & tools for
  // development.
  val stressProject = project(
    "finagle-stress", "finagle-stress",
    new StressProject(_), coreProject)

  val aprProject = project(
    "finagle-apr", "finagle-apr",
    new AprProject(_), coreProject)

  val zookeeperProject = project(
    "finagle-zookeeper", "finagle-zookeeper",
    new ZookeeperProject(_), coreProject)

  class CoreProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    override def compileOrder = CompileOrder.ScalaThenJava

    val nettyRepo =
      "repository.jboss.org" at "http://repository.jboss.org/nexus/content/groups/public/"
    val netty     = "org.jboss.netty"      %  "netty"     % "3.2.3.Final"
    val util      = "com.twitter"          %  "util"      % "1.4.10"

    val mockito   = "org.mockito"             % "mockito-all" % "1.8.5" % "test" withSources()
    val specs     = "org.scala-tools.testing" % "specs_2.8.0" % "1.6.5" % "test" withSources()
  }

  class ThriftProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with LibDirClasspath with AdhocInlines
  {
    override def compileOrder = CompileOrder.ScalaThenJava

    val thrift    = "thrift"               %  "libthrift"        % "0.5.0"
    val slf4jNop  = "org.slf4j"            %  "slf4j-nop"        % "1.5.2" % "provided"
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

  class ZookeeperProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with AdhocInlines with LibDirClasspath
  {
    override def ivyXML =
      <dependencies>
        <exclude module="jms"/>
        <exclude module="jmxtools"/>
        <exclude module="jmxri"/>
      </dependencies>

    val privateTwitterRepo = "twitter.com" at "http://svn.local.twitter.com/maven"

    val zookeeper  = "org.apache.zookeeper" % "zookeeper"    % "3.3.1"
    val serverSets = "com.twitter.common"   % "zookeeper"    % "0.0.3"
    val thrift     = "thrift"               % "libthrift"    % "0.5.0"

  }

  class AprProject(info: ProjectInfo) extends StandardProject(info)
    with SubversionPublisher with IntegrationSpecs with AdhocInlines
  {
    // This project uses tomcat-native, download at
    // http://tomcat.apache.org/download-native.cgi

    val netty     = "org.jboss.netty"         % "netty"       % "3.2.3.Final"
    val mockito   = "org.mockito"             % "mockito-all" % "1.8.5" % "test" withSources()
    val specs     = "org.scala-tools.testing" % "specs_2.8.0" % "1.6.5" % "test" withSources()
  }
}

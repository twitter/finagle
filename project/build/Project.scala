import sbt._
import com.twitter.sbt._

class Project(info: ProjectInfo) extends StandardParentProject(info)
  with SubversionPublisher
{
  // override def parallelExecution = true
  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public")

  val twitterRepo  = "twitter.com" at "http://maven.twttr.com/"

  val coreProject      = project("finagle-core",      "finagle-core",        new CoreProject(_))
  val ostrichProject   = project("finagle-ostrich",   "finagle-ostrich",     new OstrichProject(_), coreProject)
  val thriftProject    = project("finagle-thrift",    "finagle-thrift",      new ThriftProject(_), coreProject)
//  val httpProject      = project("finagle-http",      "finagle-http",      new HttpProject(_))
//  val memcachedProject = project("finagle-memcached", "finagle-memcached", new MemcachedProject(_))
//  val kestrelProject   = project("finagle-kestrel",   "finagle-kestrel",   new KestrelProject(_))
//  val hosebirdProject  = project("finagle-hosebird",  "finagle-hosebird",  new HosebirdProject(_))

  class CoreProject(info: ProjectInfo)
    extends StandardProject(info)
    with SubversionPublisher with IntegrationSpecs with AdhocInlines
  {
    override def compileOrder = CompileOrder.ScalaThenJava

    val nettyRepo =
      "repository.jboss.org" at "http://repository.jboss.org/nexus/content/groups/public/"
    val netty     = "org.jboss.netty"      %  "netty"     % "3.2.3.Final"
    val util      = "com.twitter"          %  "util"      % "1.4.8"

    val mockito   = "org.mockito"             %  "mockito-all" % "1.8.5" % "test" withSources()
    val specs     = "org.scala-tools.testing" %  "specs_2.8.0" % "1.6.5" % "test" withSources()

    val ostrich   = "com.twitter" % "ostrich" % "2.3.4"
  }

  class ThriftProject(info: ProjectInfo)
    extends StandardProject(info)
    with SubversionPublisher with LibDirClasspath with AdhocInlines
  {
    override def compileOrder = CompileOrder.ScalaThenJava

    val thrift    = "thrift"               %  "libthrift"        % "0.5.0"
    val slf4jNop  = "org.slf4j"            %  "slf4j-nop"        % "1.5.2" % "provided"
  }

  class OstrichProject(info: ProjectInfo)
    extends StandardProject(info)
    with SubversionPublisher with AdhocInlines
  {
    val ostrich = "com.twitter" % "ostrich" % "2.3.4"
  }
}

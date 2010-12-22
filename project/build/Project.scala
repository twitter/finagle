import sbt._
import com.twitter.sbt._

class Project(info: ProjectInfo)
  extends StandardProject(info)
  with LibDirClasspath
  with SubversionPublisher
  with AdhocInlines
{
  override def compileOrder = CompileOrder.ScalaThenJava
  override def managedStyle = ManagedStyle.Maven
  override def disableCrossPaths = true
  override def subversionRepository =
    Some("http://svn.local.twitter.com/maven-public")

  val nettyRepo =
    ("repository.jboss.org"
     at "http://repository.jboss.org/nexus/content/groups/public/")
  val twitterRepo  = "twitter.com" at "http://maven.twttr.com/"
  val codehausRepo = "codehaus.org" at "http://repository.codehaus.org/"

  val netty        = "org.jboss.netty"      %  "netty"            % "3.2.3.Final"
  val thrift       = "thrift"               %  "libthrift"        % "0.5.0"
  val slf4jNop     = "org.slf4j"            %  "slf4j-nop"        % "1.5.2"
  val jackson      = "org.codehaus.jackson" %  "jackson-core-asl" % "1.6.1" withSources()

  // com.twitter deps:
  val ostrich = "com.twitter" % "ostrich" % "2.3.4-SNAPSHOT"
  val util    = "com.twitter" % "util"    % "1.3.3-SNAPSHOT"

  // ** test-only
  val mockito  = "org.mockito"             %  "mockito-all" % "1.8.5" % "test" withSources()
  val specs    = "org.scala-tools.testing" %  "specs_2.8.0" % "1.6.5" % "test" withSources()
  // val killdeer = "com.twitter"             %  "killdeer"    % "0.5.1" % "test"
}

trait LibDirClasspath extends StandardProject {
  def jarFileFilter: FileFilter = "*.jar"
  def libClasspath = descendents("lib", jarFileFilter)

	override def fullUnmanagedClasspath(config: Configuration) =
    super.fullUnmanagedClasspath(config) +++ libClasspath
}

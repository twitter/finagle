import sbt._
import com.twitter.sbt._

class Project(info: ProjectInfo)
  extends StandardProject(info)
  with LibDirClasspath
{
  // ** Project inlining
  import collection.mutable.{HashSet, ListBuffer}
  import scala.collection.jcl
  val environment = jcl.Map(System.getenv())
  val inlinedLibraryDependencies = new HashSet[ModuleID]()
  val inlinedSubprojects = new ListBuffer[(String, sbt.Project)]()

  override def libraryDependencies = {
    super.libraryDependencies ++ inlinedLibraryDependencies
  }

  override def subProjects = {
    Map() ++ super.subProjects ++ inlinedSubprojects
  }

  def inline(m: ModuleID) = {
    val path = Path.fromFile("../" + m.name)
    if (environment.get("SBT_TWITTER").isDefined && path.isDirectory)
      inlinedSubprojects += (m.name -> project(path))
    else
      inlinedLibraryDependencies += m
  }
  // ~~ Project inlining

  override def compileOrder = CompileOrder.ScalaThenJava
  override def managedStyle = ManagedStyle.Maven
  override def disableCrossPaths = true

  val nettyRepo =
    ("repository.jboss.org"
     at "http://repository.jboss.org/nexus/content/groups/public/")
  val twitterRepo  = "twitter.com" at "http://maven.twttr.com/"
  val codehausRepo = "codehaus.org" at "http://repository.codehaus.org/"

  val netty        = "org.jboss.netty"      %  "netty"            % "3.2.2.Final"
  val thrift       = "thrift"               %  "libthrift"        % "0.5.0"
  val slf4jNop     = "org.slf4j"            %  "slf4j-nop"        % "1.5.2"
  val jackson      = "org.codehaus.jackson" %  "jackson-core-asl" % "1.6.1" withSources()

  // com.twitter deps:
  inline("com.twitter" % "ostrich" % "2.3.0")
  inline("com.twitter" % "util"    % "1.2.4")

  // ** test-only
  val mockito  = "org.mockito"             %  "mockito-all" % "1.8.5" % "test" withSources()
  val specs    = "org.scala-tools.testing" %  "specs_2.8.0" % "1.6.5" % "test" withSources()
  val killdeer = "com.twitter"             %  "killdeer"    % "0.5.1" % "test"
}

trait LibDirClasspath extends StandardProject {
  def jarFileFilter: FileFilter = "*.jar"
  def libClasspath = descendents("lib", jarFileFilter)

  override def compileClasspath = super.compileClasspath +++ libClasspath
  override def testClasspath = super.testClasspath +++ libClasspath
  override def runClasspath = super.runClasspath +++ libClasspath
}

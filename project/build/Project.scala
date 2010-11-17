import sbt._

class Project(info: ProjectInfo) extends DefaultProject(info) {
  // override def compileOrder = CompileOrder.JavaThenScala
  override def managedStyle = ManagedStyle.Maven
  override def disableCrossPaths = true

  val nettyRepo =
    ("repository.jboss.org"
     at "http://repository.jboss.org/nexus/content/groups/public/")
  val twitterRepo  = "twitter.com" at "http://maven.twttr.com/"
  val codehausRepo = "codehaus.org" at "http://repository.codehaus.org/"

  val ostrich      = "com.twitter"          %  "ostrich"          % "2.3.0"
  val util         = "com.twitter"          %  "util"             % "1.2.4"
  val netty        = "org.jboss.netty"      %  "netty"            % "3.2.2.Final"
  val thrift       = "thrift"               %  "libthrift"        % "0.5.0"
  val slf4jNop     = "org.slf4j"            %  "slf4j-nop"        % "1.6.1"
  val jackson      = "org.codehaus.jackson" %  "jackson-core-asl" % "1.6.1" withSources()

  // ** test-only
  val mockito  = "org.mockito"             %  "mockito-all" % "1.8.5" % "test" withSources()
  val specs    = "org.scala-tools.testing" %  "specs_2.8.0" % "1.6.5" % "test" withSources()
  val killdeer = "com.twitter"             %  "killdeer"    % "0.5.1" % "test"
}

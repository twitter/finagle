import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  import scala.collection.jcl
  val environment = jcl.Map(System.getenv())
  def isSBTOpenTwitter = environment.get("SBT_OPEN_TWITTER").isDefined
  def isSBTTwitter = environment.get("SBT_TWITTER").isDefined

  override def repositories = if (isSBTOpenTwitter) {
    Set("twitter.artifactory" at "http://artifactory.local.twitter.com/open-source/")
  } else if (isSBTTwitter) {
    Set("twitter.artifactory" at "http://artifactory.local.twitter.com/repo/")
  } else {
    super.repositories ++ Seq("twitter.com" at "http://maven.twttr.com/")
  }
  override def ivyRepositories = Seq(Resolver.defaultLocal(None)) ++ repositories

  val defaultProject = "com.twitter" % "standard-project" % "0.11.18-SNAPSHOT"
  val sbtThrift      = "com.twitter" % "sbt-thrift" % "1.2.0"
}

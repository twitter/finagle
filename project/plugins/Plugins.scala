import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  import scala.collection.jcl
  val environment = jcl.Map(System.getenv())
  def isSBTOpenTwitter = environment.get("SBT_OPEN_TWITTER").isDefined
  def isSBTTwitter = environment.get("SBT_TWITTER").isDefined
  def isInternal = isSBTOpenTwitter || isSBTTwitter

  lazy val eclipse = "de.element34" % "sbt-eclipsify" % "0.7.0"

  override def repositories = if (isSBTOpenTwitter) {
    Set("twitter.artifactory" at "http://artifactory.local.twitter.com/open-source/")
  } else if (isSBTTwitter) {
    Set("twitter.artifactory" at "http://artifactory.local.twitter.com/repo/")
  } else {
    super.repositories ++ Seq("twitter.com" at "http://maven.twttr.com/")
  }
  override def ivyRepositories =
    if (isInternal)
      Seq(Resolver.defaultLocal(None)) ++ repositories
    else
      super.ivyRepositories

  val defaultProject = "com.twitter" % "standard-project" % "1.0.4"
  val sbtThrift      = "com.twitter" % "sbt-thrift" % "2.0.1"
}

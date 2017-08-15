resolvers += Classpaths.sbtPluginReleases

val branch = Process("git" :: "rev-parse" :: "--abbrev-ref" :: "HEAD" :: Nil).!!.trim
val scroogeSbtPluginVersionPrefix = "4.19.0"
val scroogeSbtPluginVersion =
  if (branch == "master") scroogeSbtPluginVersionPrefix
  else scroogeSbtPluginVersionPrefix + "-SNAPSHOT"
addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % scroogeSbtPluginVersion)

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.3.0")
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0-RC10")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.2.27")

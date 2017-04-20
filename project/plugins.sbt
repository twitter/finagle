resolvers += Classpaths.sbtPluginReleases

val branch = Process("git" :: "rev-parse" :: "--abbrev-ref" :: "HEAD" :: Nil).!!.trim
val scroogeSbtPluginVersionPrefix = "4.16.0"
val scroogeSbtPluginVersion =
  if (branch == "master") scroogeSbtPluginVersionPrefix
  else scroogeSbtPluginVersionPrefix + "-SNAPSHOT"
addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % scroogeSbtPluginVersion)

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "0.8.1")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.2.20")

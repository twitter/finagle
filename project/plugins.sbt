resolvers += Classpaths.sbtPluginReleases
resolvers += Resolver.sonatypeRepo("snapshots")

val releaseVersion = "23.3.0-SNAPSHOT"

addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % releaseVersion)

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.3")

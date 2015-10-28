resolvers += Classpaths.sbtPluginReleases
resolvers += "twitter-repo" at "https://maven.twttr.com"

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "0.8.1")
addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % "4.1.0")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.2.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.2.2")

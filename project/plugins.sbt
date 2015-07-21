resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "0.8.1")
addSbtPlugin("com.twitter" %% "scrooge-sbt-plugin" % "3.15.0")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.4")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.2.0")

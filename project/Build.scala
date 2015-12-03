import sbt._
import Keys._
import Tests._
import com.twitter.scrooge.ScroogeSBT
import com.typesafe.sbt.SbtSite.site
import com.typesafe.sbt.site.SphinxSupport.Sphinx
import pl.project13.scala.sbt.JmhPlugin
import sbtunidoc.Plugin.UnidocKeys._
import sbtunidoc.Plugin.{ScalaUnidoc, unidocSettings}
import scoverage.ScoverageSbtPlugin

object Finagle extends Build {
  val branch = Process("git" :: "rev-parse" :: "--abbrev-ref" :: "HEAD" :: Nil).!!.trim
  val suffix = if (branch == "master") "" else "-SNAPSHOT"

  val libVersion = "6.31.0" + suffix
  val utilVersion = "6.30.0" + suffix
  val ostrichVersion = "9.14.0" + suffix
  val scroogeVersion = "4.3.0" + suffix

  val nettyLib = "io.netty" % "netty" % "3.10.1.Final"
  val netty4Libs = Seq(
    "io.netty" % "netty-handler" % "4.1.0.Beta8",
    "io.netty" % "netty-transport" % "4.1.0.Beta8"
  )
  val ostrichLib = "com.twitter" %% "ostrich" % ostrichVersion
  val jacksonVersion = "2.4.4"
  val jacksonLibs = Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion exclude("com.google.guava", "guava"),
    "com.google.guava" % "guava" % "16.0.1"
  )
  val thriftLibs = Seq(
    "org.apache.thrift" % "libthrift" % "0.5.0" intransitive(),
    "org.slf4j"   % "slf4j-api" % "1.7.7" % "provided"
  )
  val scroogeLibs = thriftLibs ++ Seq(
    "com.twitter" %% "scrooge-core" % scroogeVersion)

  def util(which: String) =
    "com.twitter" %% ("util-"+which) % utilVersion excludeAll(
      ExclusionRule(organization = "junit"),
      ExclusionRule(organization = "org.scala-tools.testing"),
      ExclusionRule(organization = "org.mockito"))

  lazy val publishM2Configuration =
    TaskKey[PublishConfiguration]("publish-m2-configuration",
      "Configuration for publishing to the .m2 repository.")

  lazy val publishM2 =
    TaskKey[Unit]("publish-m2",
      "Publishes artifacts to the .m2 repository.")

  lazy val m2Repo =
    Resolver.file("publish-m2-local",
      Path.userHome / ".m2" / "repository")

  val sharedSettings = Seq(
    version := libVersion,
    organization := "com.twitter",
    crossScalaVersions := Seq("2.10.6", "2.11.7"),
    scalaVersion := "2.11.7",
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.12.2" % "test",
      "org.scalatest" %% "scalatest" % "2.2.4" % "test",
      "junit" % "junit" % "4.10" % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test"
    ),
    resolvers += "twitter-repo" at "https://maven.twttr.com",

    ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := (
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 10)) => false
        case _ => true
      }
    ),

    publishM2Configuration <<= (packagedArtifacts, checksums in publish, ivyLoggingLevel) map { (arts, cs, level) =>
      Classpaths.publishConfig(arts, None, resolverName = m2Repo.name, checksums = cs, logging = level)
    },
    publishM2 <<= Classpaths.publishTask(publishM2Configuration, deliverLocal),
    otherResolvers += m2Repo,

    javaOptions in Test := Seq("-DSKIP_FLAKY=1"),

    ivyXML :=
      <dependencies>
        <exclude org="com.sun.jmx" module="jmxri" />
        <exclude org="com.sun.jdmk" module="jmxtools" />
        <exclude org="javax.jms" module="jms" />
      </dependencies>,

    scalacOptions ++= Seq("-encoding", "utf8"),
    scalacOptions += "-deprecation",
    scalacOptions += "-language:_",
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),
    javacOptions in doc := Seq("-source", "1.7"),

    // This is bad news for things like com.twitter.util.Time
    parallelExecution in Test := false,

    // This effectively disables packageDoc, which craps out
    // on generating docs for generated thrift due to the use
    // of raw java types.
    // packageDoc in Compile := new java.io.File("nosuchjar"),

    // Sonatype publishing
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    publishMavenStyle := true,
    autoAPIMappings := true,
    apiURL := Some(url("https://twitter.github.io/finagle/docs/")),
    pomExtra := (
      <url>https://github.com/twitter/finagle</url>
      <licenses>
        <license>
          <name>Apache License, Version 2.0</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0</url>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:twitter/finagle.git</url>
        <connection>scm:git:git@github.com:twitter/finagle.git</connection>
      </scm>
      <developers>
        <developer>
          <id>twitter</id>
          <name>Twitter Inc.</name>
          <url>https://www.twitter.com/</url>
        </developer>
      </developers>),
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },

    resourceGenerators in Compile <+=
      (resourceManaged in Compile, name, version) map { (dir, name, ver) =>
        val file = dir / "com" / "twitter" / name / "build.properties"
        val buildRev = Process("git" :: "rev-parse" :: "HEAD" :: Nil).!!.trim
        val buildName = new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(new java.util.Date)
        val contents = (
          "name=%s\nversion=%s\nbuild_revision=%s\nbuild_name=%s"
        ).format(name, ver, buildRev, buildName)
        IO.write(file, contents)
        Seq(file)
      }
  )

  val jmockSettings = Seq(
    libraryDependencies ++= Seq(
      "org.jmock" % "jmock" % "2.4.0" % "test",
      "cglib" % "cglib" % "2.2.2" % "test",
      "asm" % "asm" % "3.3.1" % "test",
      "org.objenesis" % "objenesis" % "1.1" % "test",
      "org.hamcrest" % "hamcrest-all" % "1.1" % "test"
    )
  )

  lazy val finagle = Project(
    id = "finagle",
    base = file("."),
    settings = Project.defaultSettings ++
      sharedSettings ++
      unidocSettings ++ Seq(
        unidocProjectFilter in (ScalaUnidoc, unidoc) :=
          inAnyProject -- inProjects(finagleExample)
      )
  ) aggregate(
    // Core, support.
    finagleCore, finagleTest, finagleStats, finagleNetty4,
    finagleZipkin, finagleServersets, finagleCacheResolver,
    finagleException, finagleCommonsStats,
    finagleExp, finagleMdns, finagleTesters, finagleOstrich4,

    // Protocols
    finagleHttp, finagleHttpCompat, finagleStream, finagleNative,
    finagleThrift, finagleMemcached, finagleKestrel,
    finagleMux, finagleThriftMux, finagleMySQL,
    finagleSpdy, finagleRedis

    // finagleBenchmark

    // Removing projects with specs tests and their dependencies
    // finagleExample
  )

  lazy val finagleTest = Project(
    id = "finagle-test",
    base = file("finagle-test"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-test",
    libraryDependencies ++= Seq(nettyLib, util("core"))
  )

  lazy val finagleCore = Project(
    id = "finagle-core",
    base = file("finagle-core"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-core",
    libraryDependencies ++= Seq(
      nettyLib,
      util("app"),
      util("cache"),
      util("codec"),
      util("collection"),
      util("core"),
      util("hashing"),
      util("jvm"),
      util("lint"),
      util("logging"),
      util("stats"),
      "com.twitter" % "jsr166e" % "1.0.0")
  ).dependsOn(finagleTest % "test")

  lazy val finagleNetty4 = Project(
    id = "finagle-netty4",
    base = file("finagle-netty4"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-netty4",
    libraryDependencies ++= Seq(util("core")),
    libraryDependencies ++= netty4Libs
  ).dependsOn(finagleCore)

  lazy val finagleOstrich4 = Project(
    id = "finagle-ostrich4",
    base = file("finagle-ostrich4"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-ostrich4",
    libraryDependencies ++= Seq(
      ostrichLib,
      util("registry"),
      util("stats")
    )
  ).dependsOn(finagleCore, finagleHttp)

  lazy val finagleStats = Project(
    id = "finagle-stats",
    base = file("finagle-stats"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-stats",
    libraryDependencies ++= Seq(
      "com.twitter.common" % "metrics" % "0.0.37",
      util("events"),
      util("logging"),
      util("registry"),
      util("stats")
    ),
    libraryDependencies ++= jacksonLibs
  ).dependsOn(finagleCore, finagleHttp)

  lazy val finagleZipkin = Project(
    id = "finagle-zipkin",
    base = file("finagle-zipkin"),
    settings = Project.defaultSettings ++
      ScroogeSBT.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-zipkin",
    libraryDependencies ++= Seq(util("codec"), util("events")) ++ scroogeLibs,
    libraryDependencies ++= jacksonLibs
  ).dependsOn(finagleCore, finagleThrift, finagleTest % "test")

  lazy val finagleException = Project(
    id = "finagle-exception",
    base = file("finagle-exception"),
    settings = Project.defaultSettings ++
      ScroogeSBT.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-exception",
    libraryDependencies ++= Seq(
      util("codec")
    ) ++ scroogeLibs,
    libraryDependencies ++= jacksonLibs
  ).dependsOn(finagleCore, finagleThrift)

  lazy val finagleCommonsStats = Project(
    id = "finagle-commons-stats",
    base = file("finagle-commons-stats"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-commons-stats",
    libraryDependencies ++= Seq(
      "com.twitter.common" % "stats" % "0.0.114",
      util("registry"),
      util("stats")
    )
  ).dependsOn(finagleCore)

  lazy val finagleServersets = Project(
    id = "finagle-serversets",
    base = file("finagle-serversets"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-serversets",
    fork in Test := true,
    libraryDependencies ++= Seq(
      "commons-codec" % "commons-codec" % "1.6",
      util("cache"),
      util("zk-common"),
      util("zk-test") % "test",
      "com.twitter.common.zookeeper" % "server-set" % "1.0.103",
      "com.google.guava" % "guava" % "16.0.1"
    ),

    libraryDependencies ++= jacksonLibs,
    excludeFilter in unmanagedSources := "ZkTest.scala",
    ivyXML :=
      <dependencies>
        <dependency org="com.twitter.common.zookeeper" name="server-set" rev="1.0.103">
          <exclude org="com.google.guava" name="guava"/>
          <exclude org="com.twitter" name="finagle-core"/>
          <exclude org="com.twitter" name="finagle-thrift"/>
          <exclude org="com.twitter" name="util-core"/>
          <exclude org="com.twitter" name="util-logging"/>
          <exclude org="com.twitter.common" name="jdk-logging"/>
          <exclude org="com.twitter.common" name="stats"/>
          <exclude org="com.twitter.common" name="util-executor-service-shutdown"/>
          <exclude org="io.netty" name="netty"/>
          <exclude org="javax.activation" name="activation"/>
          <exclude org="javax.mail" name="mail"/>
        </dependency>
      </dependencies>
  ).dependsOn(finagleCore, finagleTest)

  // Protocol support

  // see https://finagle.github.io/blog/2014/10/20/upgrading-finagle-to-netty-4/
  // for an explanation of the role of transitional -x packages in the netty4 migration.
  lazy val finagleHttp = Project(
    id = "finagle-http",
    base = file("finagle-http"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-http",
    libraryDependencies ++= Seq(
      util("codec"), util("logging"),
      "commons-lang" % "commons-lang" % "2.6",
      "com.google.guava" % "guava" % "16.0.1"
    )
  ).dependsOn(finagleCore)

  lazy val finagleHttpCompat = Project(
    id = "finagle-http-compat",
    base = file("finagle-http-compat"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-http-compat"
  ).dependsOn(finagleCore, finagleHttp)

  lazy val finagleNative = Project(
    id = "finagle-native",
    base = file("finagle-native"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-native"
  ).dependsOn(finagleCore, finagleHttp)

  lazy val finagleStream = Project(
    id = "finagle-stream",
    base = file("finagle-stream"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-stream"
  ).dependsOn(finagleCore, finagleTest % "test")

  lazy val finagleThrift = Project(
    id = "finagle-thrift",
    base = file("finagle-thrift"),
    settings = Project.defaultSettings ++ ScroogeSBT.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-thrift",
    libraryDependencies ++= Seq("silly" % "silly-thrift" % "0.5.0" % "test") ++ scroogeLibs
  ).dependsOn(finagleCore, finagleTest % "test")

  lazy val finagleCacheResolver = Project(
    id = "finagle-cacheresolver",
    base = file("finagle-cacheresolver"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-cacheresolver",
    libraryDependencies ++= Seq(
      "com.twitter.common" % "zookeeper-testing" % "0.0.53" % "test"
    ),
    libraryDependencies ++= jacksonLibs
  ).dependsOn(finagleCore, finagleServersets)

  lazy val finagleMemcached = Project(
    id = "finagle-memcached",
    base = file("finagle-memcached"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-memcached",
    libraryDependencies ++= Seq(
      util("hashing"),
      util("zk-test") % "test",
      "com.google.guava" % "guava" % "16.0.1",
      "com.twitter.common" % "zookeeper-testing" % "0.0.53" % "test"
    ),
    libraryDependencies ++= jacksonLibs
  ).dependsOn(finagleCacheResolver, finagleCore, finagleServersets)

  lazy val finagleKestrel = Project(
    id = "finagle-kestrel",
    base = file("finagle-kestrel"),
    settings = Project.defaultSettings ++
      ScroogeSBT.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-kestrel",
    libraryDependencies ++= scroogeLibs
  ).dependsOn(finagleCore, finagleMemcached, finagleThrift)

  lazy val finagleRedis = Project(
    id = "finagle-redis",
    base = file("finagle-redis"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-redis",
    libraryDependencies ++= Seq(
      util("logging")
    ),
    testOptions in Test := Seq(Tests.Filter {
      name => !name.startsWith("com.twitter.finagle.redis.integration")
    })
  ).dependsOn(finagleCore)

  lazy val finagleMux = Project(
    id = "finagle-mux",
    base = file("finagle-mux"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-mux",
    libraryDependencies ++= Seq("com.twitter.common" % "stats-util" % "0.0.58")
  ).dependsOn(finagleCore)

  lazy val finagleThriftMux = Project(
    id = "finagle-thriftmux",
    base = file("finagle-thriftmux"),
    settings = Project.defaultSettings ++
      ScroogeSBT.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-thriftmux",
    libraryDependencies ++= scroogeLibs
  ).dependsOn(finagleCore, finagleMux, finagleThrift)

  lazy val finagleMySQL = Project(
    id = "finagle-mysql",
    base = file("finagle-mysql"),
    settings = Project.defaultSettings ++
      sharedSettings
    ).settings(
      name := "finagle-mysql",
      libraryDependencies ++= Seq(util("logging"), util("cache")),
      excludeFilter in unmanagedSources := { "EmbeddableMysql.scala" || "ClientTest.scala" }
    ).dependsOn(finagleCore)

  lazy val finagleExp = Project(
    id = "finagle-exp",
    base = file("finagle-exp"),
    settings = Project.defaultSettings ++
      sharedSettings
    ).settings(
      name := "finagle-exp",
      libraryDependencies ++= Seq(
        "com.twitter" % "jsr166e" % "1.0.0"
      )
    ).dependsOn(finagleCore, finagleThrift, finagleTest % "test")

  // Uses

  lazy val finagleMdns = Project(
    id = "finagle-mdns",
    base = file("finagle-mdns"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-mdns",
    libraryDependencies += "javax.jmdns" % "jmdns" % "3.4.1"
  ).dependsOn(finagleCore)

  lazy val finagleExample = Project(
    id = "finagle-example",
    base = file("finagle-example"),
    settings = Project.defaultSettings ++
      ScroogeSBT.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-example",
    libraryDependencies ++= Seq(
      util("codec"),
      "org.slf4j" %  "slf4j-nop" % "1.7.7" % "provided"
    ) ++ scroogeLibs
  ).dependsOn(
    finagleCore, finagleThrift, finagleMemcached, finagleKestrel,
    finagleRedis, finagleMySQL, finagleOstrich4, finagleStats)

  lazy val finagleBenchmarkThrift = Project(
    id = "finagle-benchmark-thrift",
    base = file("finagle-benchmark-thrift"),
    settings = Project.defaultSettings ++
      sharedSettings ++ ScroogeSBT.newSettings
  ).settings(
    libraryDependencies ++= scroogeLibs
  ).dependsOn(finagleThrift)

  lazy val finagleBenchmark = Project(
    id = "finagle-benchmark",
    base = file("finagle-benchmark"),
    settings = Project.defaultSettings ++
      sharedSettings ++ JmhPlugin.projectSettings
  )
  .enablePlugins(JmhPlugin)
  .settings(
    name := "finagle-benchmark",
    libraryDependencies ++= Seq(
      util("codec"),
      "com.twitter.common" % "metrics-data-sample" % "0.0.1",
      "org.apache.curator" % "curator-test" % "2.8.0",
      "org.apache.curator" % "curator-framework" % "2.8.0"
    )
  ).dependsOn(
    finagleBenchmarkThrift,
    finagleCommonsStats,
    finagleCore,
    finagleExp,
    finagleMemcached,
    finagleMux,
    finagleOstrich4,
    finagleStats,
    finagleThriftMux,
    finagleZipkin
  )

  lazy val finagleTesters = Project(
    id = "finagle-testers",
    base = file("finagle-testers"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-testers"
  ).dependsOn(finagleCore)

  lazy val finagleSpdy = Project(
    id = "finagle-spdy",
    base = file("finagle-spdy"),
    settings = Project.defaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-spdy"
  ).dependsOn(finagleCore)

  lazy val finagleDoc = Project(
    id = "finagle-doc",
    base = file("doc"),
    settings = Project.defaultSettings ++ site.settings ++ site.sphinxSupport() ++ sharedSettings ++ Seq(
      scalacOptions in doc <++= (version).map(v => Seq("-doc-title", "Finagle", "-doc-version", v)),
      includeFilter in Sphinx := ("*.html" | "*.png" | "*.svg" | "*.js" | "*.css" | "*.gif" | "*.txt"),

      // Workaround for sbt bug: Without a testGrouping for all test configs,
      // the wrong tests are run
      testGrouping <<= definedTests in Test map partitionTests,
      testGrouping in DocTest <<= definedTests in DocTest map partitionTests

    )).configs(DocTest).settings(inConfig(DocTest)(Defaults.testSettings): _*).settings(
    unmanagedSourceDirectories in DocTest <+= baseDirectory { _ / "src/sphinx/code" },
    //resourceDirectory in DocTest <<= baseDirectory { _ / "src/test/resources" }

    // Make the "test" command run both, test and doctest:test
    test <<= Seq(test in Test, test in DocTest).dependOn
    ).dependsOn(finagleCore, finagleHttp, finagleMySQL)

  /* Test Configuration for running tests on doc sources */
  lazy val DocTest = config("doctest") extend(Test)

  // A dummy partitioning scheme for tests
  def partitionTests(tests: Seq[TestDefinition]) = {
    Seq(new Group("inProcess", tests, InProcess))
  }
}

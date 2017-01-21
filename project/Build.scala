import sbt._
import Keys._
import Tests._
import com.twitter.scrooge.ScroogeSBT
import com.typesafe.sbt.SbtSite.site
import com.typesafe.sbt.site.SphinxSupport.Sphinx
import pl.project13.scala.sbt.JmhPlugin
import sbtunidoc.Plugin.UnidocKeys._
import sbtunidoc.Plugin.{ScalaUnidoc, unidocSettings}
import scoverage.ScoverageKeys

object Finagle extends Build {
  val branch = Process("git" :: "rev-parse" :: "--abbrev-ref" :: "HEAD" :: Nil).!!.trim
  val suffix = if (branch == "master") "" else "-SNAPSHOT"

  val libVersion = "6.41.0" + suffix
  val utilVersion = "6.40.0" + suffix
  val ostrichVersion = "9.24.0" + suffix
  val scroogeVersion = "4.13.0" + suffix

  val libthriftVersion = "0.5.0-7"

  val netty4Version = "4.1.7.Final"

  val guavaLib = "com.google.guava" % "guava" % "16.0.1"
  val caffeineLib = "com.github.ben-manes.caffeine" % "caffeine" % "2.3.4"
  val jsr305Lib = "com.google.code.findbugs" % "jsr305" % "2.0.1"
  val nettyLib = "io.netty" % "netty" % "3.10.1.Final"
  val netty4Libs = Seq(
    "io.netty" % "netty-handler" % netty4Version,
    "io.netty" % "netty-transport" % netty4Version,
    "io.netty" % "netty-handler-proxy" % netty4Version
  )
  val netty4Http = "io.netty" % "netty-codec-http" % netty4Version
  val netty4Http2 = "io.netty" % "netty-codec-http2" % netty4Version
  val ostrichLib = "com.twitter" %% "ostrich" % ostrichVersion
  val jacksonVersion = "2.8.4"
  val jacksonLibs = Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion exclude("com.google.guava", "guava"),
    guavaLib
  )
  val thriftLibs = Seq(
    "com.twitter" % "libthrift" % libthriftVersion intransitive(),
    "org.slf4j" % "slf4j-api" % "1.7.7" % "provided"
  )
  val scroogeLibs = thriftLibs ++ Seq(
    "com.twitter" %% "scrooge-core" % scroogeVersion)

  def util(which: String) =
    "com.twitter" %% ("util-"+which) % utilVersion excludeAll(
      ExclusionRule(organization = "junit"),
      ExclusionRule(organization = "org.scala-tools.testing"),
      ExclusionRule(organization = "org.mockito"))

  val sharedSettings = Seq(
    version := libVersion,
    organization := "com.twitter",
    scalaVersion := "2.12.1",
    crossScalaVersions := Seq("2.11.8", "2.12.1"),
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "junit" % "junit" % "4.10" % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test"
    ),

    ScoverageKeys.coverageHighlighting := true,
    ScroogeSBT.autoImport.scroogeLanguages in Test := Seq("java", "scala"),

    javaOptions in Test := Seq("-DSKIP_FLAKY=1"),

    ivyXML :=
      <dependencies>
        <exclude org="com.sun.jmx" module="jmxri" />
        <exclude org="com.sun.jdmk" module="jmxtools" />
        <exclude org="javax.jms" module="jms" />
      </dependencies>,

    scalacOptions := Seq(
      // Note: Add -deprecation when deprecated methods are removed
      "-target:jvm-1.8",
      "-unchecked",
      "-feature",
      "-language:_",
      "-encoding", "utf8",
      "-Xlint:-missing-interpolator",
      "-Ypatmat-exhaust-depth", "40"
    ),
    javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.8", "-target", "1.8"),
    javacOptions in doc := Seq("-source", "1.8"),

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
    pomExtra :=
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
      </developers>,
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },

    // Prevent eviction warnings
    dependencyOverrides <++= scalaVersion { vsn =>
      Set(
        "com.twitter" % "libthrift" % libthriftVersion
      )
    },

    resourceGenerators in Compile <+=
      (resourceManaged in Compile, name, version) map { (dir, name, ver) =>
        val file = dir / "com" / "twitter" / name / "build.properties"
        val buildRev = Process("git" :: "rev-parse" :: "HEAD" :: Nil).!!.trim
        val buildName = new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(new java.util.Date)
        val contents = s"name=$name\nversion=$ver\nbuild_revision=$buildRev\nbuild_name=$buildName"
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

  lazy val projectList = Seq[sbt.ProjectReference](
    // Core, support.
    finagleToggle,
    finagleCore,
    finagleStats,
    finagleNetty4,
    finagleZipkinCore,
    finagleZipkin,
    finagleServersets,
    finagleException,
    finagleIntegration,
    finagleCommonsStats,
    finagleExp,
    finagleMdns,
    finagleOstrich4,

    // Protocols
    finagleHttp,
    finagleBaseHttp,
    finagleHttp2,
    finagleHttpCompat,
    finagleStream,
    finagleNative,
    finagleThrift,
    finagleMemcached,
    finagleKestrel,
    finagleMux,
    finagleThriftMux,
    finagleMySQL,
    finagleSpdy,
    finagleRedis,
    finagleNetty4Http
  )

  lazy val finagle = Project(
    id = "finagle",
    base = file("."),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings ++
      unidocSettings ++ Seq(
        unidocProjectFilter in (ScalaUnidoc, unidoc) :=
          inAnyProject -- inProjects(finagleExample)
      )
  ).aggregate(projectList: _*)

  lazy val finagleIntegration = Project(
    id = "finagle-integration",
    base = file("finagle-integration"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-integration",
    libraryDependencies ++= Seq(util("core")) ++ scroogeLibs
  ).dependsOn(
    finagleCore,
    finagleHttp,
    finagleHttp2,
    finagleMySQL,
    finagleMemcached,
    finagleMux,
    finagleNetty4Http,
    finagleThrift,
    finagleThriftMux % "test->compile;test->test"
  )

  lazy val finagleToggle = Project(
    id = "finagle-toggle",
    base = file("finagle-toggle"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-toggle",
    libraryDependencies ++= Seq(
      util("app"),
      util("core"),
      util("logging"),
      util("registry"),
      util("stats")) ++
      jacksonLibs
  )

  lazy val finagleCore = Project(
    id = "finagle-core",
    base = file("finagle-core"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-core",
    libraryDependencies ++= Seq(
      util("app"),
      util("cache"),
      util("codec"),
      util("core"),
      util("hashing"),
      util("jvm"),
      util("lint"),
      util("logging"),
      util("registry"),
      util("security"),
      util("stats"),
      caffeineLib,
      jsr305Lib,
      nettyLib)
  )

  lazy val finagleNetty4 = Project(
    id = "finagle-netty4",
    base = file("finagle-netty4"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-netty4",
    libraryDependencies ++= Seq(
      util("app"),
      util("cache"),
      util("codec"),
      util("core"),
      util("codec"),
      util("stats"),
      netty4Http
    ) ++ netty4Libs
  ).dependsOn(finagleCore, finagleToggle)

  lazy val finagleOstrich4 = Project(
    id = "finagle-ostrich4",
    base = file("finagle-ostrich4"),
    settings = Defaults.coreDefaultSettings ++
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
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-stats",
    libraryDependencies ++= Seq(
      "com.twitter.common" % "metrics" % "0.0.38",
      util("app"),
      util("core"),
      util("events"),
      util("lint"),
      util("logging"),
      util("registry"),
      util("stats")
    ),
    libraryDependencies ++= jacksonLibs
  ).dependsOn(
    finagleCore,
    finagleHttp,
    finagleToggle)

  lazy val finagleZipkinCore = Project(
    id = "finagle-zipkin-core",
    base = file("finagle-zipkin-core"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-zipkin-core",
    libraryDependencies ++= Seq(
      util("codec"),
      util("events"),
      util("core"),
      util("stats")) ++ scroogeLibs ++ jacksonLibs
  ).dependsOn(finagleCore, finagleThrift)

  lazy val finagleZipkin = Project(
    id = "finagle-zipkin",
    base = file("finagle-zipkin"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-zipkin",
    libraryDependencies ++= scroogeLibs
  ).dependsOn(finagleCore, finagleThrift, finagleZipkinCore)

  lazy val finagleException = Project(
    id = "finagle-exception",
    base = file("finagle-exception"),
    settings = Defaults.coreDefaultSettings ++
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
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-commons-stats",
    libraryDependencies ++= Seq(
      "com.twitter.common" % "stats" % "0.0.115",
      util("core"),
      util("registry"),
      util("stats")
    ),
    libraryDependencies ++= jacksonLibs
  ).dependsOn(finagleCore, finagleHttp)

  lazy val finagleServersets = Project(
    id = "finagle-serversets",
    base = file("finagle-serversets"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-serversets",
    fork in Test := true,
    libraryDependencies ++= Seq(
      caffeineLib,
      util("cache"),
      util("zk-common"),
      util("zk-test") % "test",
      "com.twitter" % "libthrift" % libthriftVersion,
      "com.twitter.common" % "io-json" % "0.0.54",
      "com.twitter.common.zookeeper" % "server-set" % "1.0.111" excludeAll(
        ExclusionRule("com.twitter", "finagle-core-java"),
        ExclusionRule("com.twitter", "finagle-core_2.11"),
        ExclusionRule("com.twitter", "util-core-java"),
        ExclusionRule("com.twitter", "util-core_2.11"),
        ExclusionRule("com.twitter.common", "service-thrift"),
        ExclusionRule("org.apache.thrift", "libthrift"),
        ExclusionRule("org.apache.zookeeper", "zookeeper"),
        ExclusionRule("org.apache.zookeeper", "zookeeper-client"),
        ExclusionRule("org.scala-lang.modules", "scala-parser-combinators_2.11")
      ),
      "com.twitter.common" % "service-thrift" % "1.0.55" excludeAll(
        ExclusionRule("org.apache.thrift", "libthrift")
      ),
      guavaLib
    ),
    libraryDependencies ++= jacksonLibs,
    excludeFilter in unmanagedSources := "ZkTest.scala"
  ).dependsOn(finagleCore)

  // Protocol support

  lazy val finagleHttp = Project(
    id = "finagle-http",
    base = file("finagle-http"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-http",
    libraryDependencies ++= Seq(
      util("codec"),
      util("collection"),
      util("logging"),
      "commons-lang" % "commons-lang" % "2.6",
      guavaLib
    )
  ).dependsOn(finagleBaseHttp, finagleNetty4Http, finagleToggle)

  lazy val finagleBaseHttp = Project(
    id = "finagle-base-http",
    base = file("finagle-base-http"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-base-http",
    libraryDependencies ++= Seq(
      util("collection"),
      util("logging"),
      "commons-lang" % "commons-lang" % "2.6"
    )
  ).dependsOn(finagleCore)

  lazy val finagleNetty4Http = Project(
    id = "finagle-netty4-http",
    base = file("finagle-netty4-http"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-netty4-http",
    libraryDependencies ++= Seq(
      util("app"), util("codec"), util("core"), util("jvm"), util("stats"),
      "commons-lang" % "commons-lang" % "2.6",
      netty4Http
    )
  ).dependsOn(finagleNetty4, finagleBaseHttp)

  lazy val finagleHttp2 = Project(
    id = "finagle-http2",
    base = file("finagle-http2"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-http2",
    libraryDependencies ++= Seq(
      netty4Http2,
      util("cache"),
      util("core"),
      util("logging"),
      nettyLib
    ) ++ netty4Libs
  ).dependsOn(finagleCore, finagleHttp % "test->test;compile->compile", finagleNetty4, finagleNetty4Http)

  lazy val finagleHttpCompat = Project(
    id = "finagle-http-compat",
    base = file("finagle-http-compat"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-http-compat"
  ).dependsOn(finagleCore, finagleHttp)

  lazy val finagleNative = Project(
    id = "finagle-native",
    base = file("finagle-native"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-native"
  ).dependsOn(finagleCore, finagleHttp)

  lazy val finagleStream = Project(
    id = "finagle-stream",
    base = file("finagle-stream"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-stream"
  ).dependsOn(finagleCore)

  lazy val finagleThrift = Project(
    id = "finagle-thrift",
    base = file("finagle-thrift"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-thrift",
    libraryDependencies ++=
      Seq(
        "commons-lang" % "commons-lang" % "2.6" % "test") ++ scroogeLibs
  ).dependsOn(finagleCore, finagleNetty4, finagleToggle)

  lazy val finagleMemcached = Project(
    id = "finagle-memcached",
    base = file("finagle-memcached"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-memcached",
    libraryDependencies ++= Seq(
      util("hashing"),
      util("zk-test") % "test",
      guavaLib,
      "com.twitter" %% "bijection-core" % "0.9.4",
      "com.twitter.common" % "io-json" % "0.0.54",
      "com.twitter.common" % "zookeeper-testing" % "0.0.56" % "test" excludeAll(
        // These exclusions are necessary because zookeeper-testing has an explicit
        // dependency on scala 2.11 and some 2.11 scala libraries
        ExclusionRule("com.google.testing", "test-libraries-for-java"),
        ExclusionRule("com.twitter", "finagle-core-java"),
        ExclusionRule("com.twitter", "finagle-core_2.11"),
        ExclusionRule("com.twitter", "finagle-core_2.12"),
        ExclusionRule("com.twitter", "finagle-http_2.11"),
        ExclusionRule("com.twitter", "finagle-http_2.12"),
        ExclusionRule("com.twitter", "finagle-http-java"),
        ExclusionRule("com.twitter", "finagle-stats_2.11"),
        ExclusionRule("com.twitter", "finagle-stats_2.12"),
        ExclusionRule("com.twitter", "finagle-toggle_2.11"),
        ExclusionRule("com.twitter", "finagle-toggle_2.12"),
        ExclusionRule("com.twitter", "util-app_2.11"),
        ExclusionRule("com.twitter", "util-app_2.12"),
        ExclusionRule("com.twitter", "util-codec_2.11"),
        ExclusionRule("com.twitter", "util-codec_2.12"),
        ExclusionRule("com.twitter", "util-collection_2.11"),
        ExclusionRule("com.twitter", "util-collection_2.12"),
        ExclusionRule("com.twitter", "util-core-java"),
        ExclusionRule("com.twitter", "util-core_2.11"),
        ExclusionRule("com.twitter", "util-core_2.12"),
        ExclusionRule("com.twitter", "util-lint_2.11"),
        ExclusionRule("com.twitter", "util-list_2.12"),
        ExclusionRule("com.twitter", "util-logging_2.11"),
        ExclusionRule("com.twitter", "util-logging_2.12"),
        ExclusionRule("com.twitter", "util-registry_2.11"),
        ExclusionRule("com.twitter", "util-registry_2.12"),
        ExclusionRule("com.twitter", "util-stats_2.11"),
        ExclusionRule("com.twitter", "util-stats_2.12"),
        ExclusionRule("com.twitter", "twitter-server_2.11"),
        ExclusionRule("com.twitter.common", "metrics"),
        ExclusionRule("com.twitter.common", "service-thrift"),
        ExclusionRule("org.apache.thrift", "libthrift"),
        ExclusionRule("org.apache.zookeeper", "zookeeper"),
        ExclusionRule("org.apache.zookeeper", "zookeeper-client"),
        ExclusionRule("org.apache.zookeeper", "zookeeper-server"),
        ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala_2.11"),
        ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala_2.12"),
        ExclusionRule("org.scala-lang.modules", "scala-parser-combinators_2.11"),
        ExclusionRule("org.scala-lang.modules", "scala-parser-combinators_2.12"),
        ExclusionRule("org.scala-lang.modules", "scala-xml_2.11"),
        ExclusionRule("org.scala-lang.modules", "scala-xml_2.12")
      ),
      "com.twitter" % "libthrift" % libthriftVersion
    ),
    libraryDependencies ++= jacksonLibs
  ).dependsOn(
  finagleCore,
  finagleNetty4,
  finagleServersets,
  finagleStats,
  finagleToggle)

  lazy val finagleKestrel = Project(
    id = "finagle-kestrel",
    base = file("finagle-kestrel"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-kestrel",
    libraryDependencies ++= scroogeLibs :+ caffeineLib
  ).dependsOn(
    finagleCore,
    finagleMemcached,
    finagleNetty4,
    finagleThrift,
    finagleThriftMux,
    finagleToggle)

  lazy val finagleRedis = Project(
    id = "finagle-redis",
    base = file("finagle-redis"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-redis",
    libraryDependencies ++= Seq(
      util("logging")
    ),
    testOptions in Test := Seq(Tests.Filter {
      name => !name.startsWith("com.twitter.finagle.redis.integration")
    })
  ).dependsOn(finagleCore, finagleNetty4)

  lazy val finagleMux = Project(
    id = "finagle-mux",
    base = file("finagle-mux"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-mux",
    libraryDependencies ++= Seq(
      util("app"),
      util("core"),
      util("logging"),
      util("stats"),
      "com.twitter.common" % "stats-util" % "0.0.59")
  ).dependsOn(
    finagleCore,
    finagleNetty4,
    finagleToggle)

  lazy val finagleThriftMux = Project(
    id = "finagle-thriftmux",
    base = file("finagle-thriftmux"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-thriftmux",
    libraryDependencies ++= Seq(
      util("core"),
      util("logging"),
      util("stats")) ++ scroogeLibs
  ).dependsOn(finagleCore, finagleMux, finagleThrift)

  lazy val finagleMySQL = Project(
    id = "finagle-mysql",
    base = file("finagle-mysql"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
    ).settings(
      name := "finagle-mysql",
      libraryDependencies ++= Seq(util("logging"), util("cache"), caffeineLib, jsr305Lib),
      excludeFilter in unmanagedSources := { "EmbeddableMysql.scala" || "ClientTest.scala" }
    ).dependsOn(finagleCore, finagleNetty4, finagleToggle)

  lazy val finagleExp = Project(
    id = "finagle-exp",
    base = file("finagle-exp"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
    ).settings(
      name := "finagle-exp"
    ).dependsOn(finagleCore, finagleThrift)

  lazy val finagleMdns = Project(
    id = "finagle-mdns",
    base = file("finagle-mdns"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-mdns",
    libraryDependencies += "org.jmdns" % "jmdns" % "3.5.1"
  ).dependsOn(finagleCore)

  lazy val finagleExample = Project(
    id = "finagle-example",
    base = file("finagle-example"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-example",
    libraryDependencies ++= Seq(
      util("codec"),
      "org.slf4j" %  "slf4j-nop" % "1.7.7" % "provided"
    ) ++ scroogeLibs
  ).dependsOn(
    finagleCore,
    finagleHttp,
    finagleMemcached,
    finagleKestrel,
    finagleMySQL,
    finagleOstrich4,
    finagleRedis,
    finagleStats,
    finagleThrift)

  lazy val finagleBenchmarkThrift = Project(
    id = "finagle-benchmark-thrift",
    base = file("finagle-benchmark-thrift"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    libraryDependencies ++= scroogeLibs
  ).dependsOn(finagleThrift)

  lazy val finagleBenchmark = Project(
    id = "finagle-benchmark",
    base = file("finagle-benchmark"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings ++ JmhPlugin.projectSettings
  )
  .enablePlugins(JmhPlugin)
  .settings(
    name := "finagle-benchmark",
    libraryDependencies ++= Seq(
      util("codec"),
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
    finagleNetty4,
    finagleOstrich4,
    finagleStats,
    finagleThriftMux,
    finagleZipkin
  ).aggregate(finagleBenchmarkThrift)

  lazy val finagleSpdy = Project(
    id = "finagle-spdy",
    base = file("finagle-spdy"),
    settings = Defaults.coreDefaultSettings ++
      sharedSettings
  ).settings(
    name := "finagle-spdy"
  ).dependsOn(finagleCore)

  lazy val finagleDoc = Project(
    id = "finagle-doc",
    base = file("doc"),
    settings = Defaults.coreDefaultSettings ++ site.settings ++ site.sphinxSupport() ++ sharedSettings ++ Seq(
      scalacOptions in doc <++= version.map(v => Seq("-doc-title", "Finagle", "-doc-version", v)),
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
  lazy val DocTest = config("doctest") extend Test

  // A dummy partitioning scheme for tests
  def partitionTests(tests: Seq[TestDefinition]) = {
    Seq(new Group("inProcess", tests, InProcess))
  }
}

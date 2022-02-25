import Tests._
import scoverage.ScoverageKeys

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / excludeLintKeys += scalacOptions

// All Twitter library releases are date versioned as YY.MM.patch
val releaseVersion = "22.2.0"

val libthriftVersion = "0.10.0"

val defaultNetty4Version = "4.1.73.Final"
val defaultNetty4StaticSslVersion = "2.0.46.Final"

val useNettySnapshot: Boolean = sys.env.get("FINAGLE_USE_NETTY_4_SNAPSHOT") match {
  case Some(useSnapshot) => useSnapshot.toBoolean
  case _ => false
}

val extraSnapshotResolvers = Seq(Resolver.sonatypeRepo("snapshots"))

val netty4Version: String =
  if (useNettySnapshot) {
    sys.env("FINAGLE_NETTY_4_VERSION")
  } else {
    defaultNetty4Version
  }

val netty4StaticSslVersion: String =
  if (useNettySnapshot) {
    sys.env("FINAGLE_NETTY_4_TCNATIVE_VERSION")
  } else {
    defaultNetty4StaticSslVersion
  }

val nettyVersionInfo = settingKey[String]("A setting reference for printing the netty version info")

// zkVersion should be kept in sync with the 'util-zk' dependency version
val zkVersion = "3.5.6"

val scalaCollectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.2"
val caffeineLib = "com.github.ben-manes.caffeine" % "caffeine" % "2.9.3"
val hdrHistogramLib = "org.hdrhistogram" % "HdrHistogram" % "2.1.11"
val jsr305Lib = "com.google.code.findbugs" % "jsr305" % "2.0.1"
val netty4StaticSsl = "io.netty" % "netty-tcnative-boringssl-static" % netty4StaticSslVersion
val netty4Libs = Seq(
  "io.netty" % "netty-handler" % netty4Version,
  "io.netty" % "netty-transport" % netty4Version,
  "io.netty" % "netty-transport-native-epoll" % netty4Version classifier "linux-x86_64",
  "io.netty" % "netty-transport-native-epoll" % netty4Version classifier "linux-aarch_64",
  // this package is a dep of native-epoll above, explicitly add this for coursier plugin
  "io.netty" % "netty-transport-native-unix-common" % netty4Version,
  "io.netty" % "netty-handler-proxy" % netty4Version
)
val netty4LibsTest = Seq(
  "io.netty" % "netty-handler" % netty4Version % "test",
  "io.netty" % "netty-transport" % netty4Version % "test",
  "io.netty" % "netty-transport-native-epoll" % netty4Version % "test" classifier "linux-x86_64",
  "io.netty" % "netty-transport-native-epoll" % netty4Version % "test" classifier "linux-aarch_64",
  // this package is a dep of native-epoll above, explicitly add this for coursier plugin
  "io.netty" % "netty-transport-native-unix-common" % netty4Version % "test",
  "io.netty" % "netty-handler-proxy" % netty4Version % "test",
  netty4StaticSsl % "test"
)
val netty4Http = "io.netty" % "netty-codec-http" % netty4Version
val netty4Http2 = "io.netty" % "netty-codec-http2" % netty4Version
val opencensusVersion = "0.24.0"
val jacksonVersion = "2.13.1"
val jacksonLibs = Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion exclude ("com.google.guava", "guava")
)
val thriftLibs = Seq(
  "org.apache.thrift" % "libthrift" % libthriftVersion intransitive ()
)
val scroogeLibs = thriftLibs ++ Seq("com.twitter" %% "scrooge-core" % releaseVersion)

val lz4Lib = "org.lz4" % "lz4-java" % "1.6.0"

def util(which: String) =
  "com.twitter" %% ("util-" + which) % releaseVersion excludeAll (ExclusionRule(organization =
    "junit"),
  ExclusionRule(organization = "org.scala-tools.testing"),
  ExclusionRule(organization = "org.mockito"))

def travisTestJavaOptions: Seq[String] = {
  // We have some custom configuration for the Travis environment
  // https://docs.travis-ci.com/user/environment-variables/#default-environment-variables
  val travisBuild = sys.env.getOrElse("TRAVIS", "false").toBoolean
  if (travisBuild) {
    Seq(
      "-DSKIP_FLAKY=true",
      "-DSKIP_FLAKY_TRAVIS=true"
    )
  } else {
    Seq(
      "-DSKIP_FLAKY=true"
    )
  }
}

def gcJavaOptions: Seq[String] = {
  val javaVersion = System.getProperty("java.version")
  if (javaVersion.startsWith("1.8")) {
    jdk8GcJavaOptions
  } else {
    jdk11GcJavaOptions
  }
}

def jdk8GcJavaOptions: Seq[String] = {
  Seq(
    "-XX:+UseParNewGC",
    "-XX:+UseConcMarkSweepGC",
    "-XX:+CMSParallelRemarkEnabled",
    "-XX:+CMSClassUnloadingEnabled",
    "-XX:ReservedCodeCacheSize=128m",
    "-XX:SurvivorRatio=128",
    "-XX:MaxTenuringThreshold=0",
    "-Xss8M",
    "-Xms512M",
    "-Xmx3G"
  )
}

def jdk11GcJavaOptions: Seq[String] = {
  Seq(
    "-XX:+UseConcMarkSweepGC",
    "-XX:+CMSParallelRemarkEnabled",
    "-XX:+CMSClassUnloadingEnabled",
    "-XX:ReservedCodeCacheSize=128m",
    "-XX:SurvivorRatio=128",
    "-XX:MaxTenuringThreshold=0",
    "-Xss8M",
    "-Xms512M",
    "-Xmx3G"
  )
}

val sharedSettings = Seq(
  version := releaseVersion,
  organization := "com.twitter",
  scalaVersion := "2.13.6",
  crossScalaVersions := Seq("2.12.12", "2.13.6"),
  Test / fork := true, // We have to fork to get the JavaOptions
  libraryDependencies ++= Seq(
    // See https://www.scala-sbt.org/0.13/docs/Testing.html#JUnit
    "com.novocode" % "junit-interface" % "0.11" % "test",
    "org.scalacheck" %% "scalacheck" % "1.15.4" % "test",
    "org.scalatest" %% "scalatest" % "3.1.1" % "test",
    "org.scalatestplus" %% "junit-4-12" % "3.1.2.0" % "test",
    "org.scalatestplus" %% "mockito-1-10" % "3.1.0.0" % "test",
    "org.scalatestplus" %% "scalacheck-1-14" % "3.1.2.0" % "test",
    scalaCollectionCompat
  ),
  // Workaround for cross building Dtab.scala, which is not compatible between
  // 2.12- with 2.13+.
  Compile / unmanagedSourceDirectories += {
    val sourceDir = (Compile / sourceDirectory).value
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, n)) if n >= 13 => sourceDir / "scala-2.13+"
      case _ => sourceDir / "scala-2.12-"
    }
  },
  ScoverageKeys.coverageHighlighting := true,
  Test / ScroogeSBT.autoImport.scroogeLanguages := Seq("java", "scala"),
  ivyXML :=
    <dependencies>
      <exclude org="com.sun.jmx" module="jmxri" />
      <exclude org="com.sun.jdmk" module="jmxtools" />
      <exclude org="javax.jms" module="jms" />
    </dependencies>,
  scalacOptions := Seq(
    "-target:jvm-1.8",
    "-deprecation",
    "-unchecked",
    "-feature",
    "-language:_",
    "-encoding",
    "utf8",
    "-Xlint:-missing-interpolator",
    "-Ypatmat-exhaust-depth",
    "40"
  ),
  javacOptions ++= Seq(
    "-Xlint:unchecked",
    "-source",
    "1.8",
    "-target",
    "1.8"
  ),
  doc / javacOptions := Seq("-source", "1.8"),
  javaOptions ++= Seq(
    "-Djava.net.preferIPv4Stack=true",
    "-DSKIP_SBT=1",
    "-XX:+AggressiveOpts",
    "-server"
  ),
  javaOptions ++= gcJavaOptions,
  Test / javaOptions ++= travisTestJavaOptions,
  // This is bad news for things like com.twitter.util.Time
  Test / parallelExecution := false,
  // -a: print stack traces for failing asserts
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-a"),
  resolvers ++= extraSnapshotResolvers,
  // This effectively disables packageDoc, which craps out
  // on generating docs for generated thrift due to the use
  // of raw java types.
  // Compile / packageDoc := new java.io.File("nosuchjar"),

  // Sonatype publishing
  Test / publishArtifact := false,
  pomIncludeRepository := { _ => false },
  publishMavenStyle := true,
  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
  autoAPIMappings := true,
  apiURL := Some(url("https://twitter.github.io/finagle/docs/")),
  pomExtra :=
    <url>https://github.com/twitter/finagle</url>
    <licenses>
      <license>
        <name>Apache License, Version 2.0</name>
        <url>https://www.apache.org/licenses/LICENSE-2.0</url>
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
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  // Prevent eviction warnings
  dependencyOverrides ++= (scalaVersion { vsn =>
    Seq(
      "org.apache.thrift" % "libthrift" % libthriftVersion
    )
  }).value,
  Compile / resourceGenerators += Def.task {
    val dir = (Compile / resourceManaged).value
    val file = dir / "com" / "twitter" / name.value / "build.properties"
    val buildRev = scala.sys.process.Process("git" :: "rev-parse" :: "HEAD" :: Nil).!!.trim
    val buildName = new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(new java.util.Date)
    val contents =
      s"name=${name.value}\nversion=${version.value}\nbuild_revision=$buildRev\nbuild_name=$buildName"
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

lazy val noPublishSettings = Seq(
  publish / skip := true
)

lazy val projectList = Seq[sbt.ProjectReference](
  // Core, support.
  finagleToggle,
  finagleCore,
  finagleNetty4,
  finagleStats,
  finagleStatsCore,
  finagleZipkinCore,
  finagleZipkinScribe,
  finagleServersets,
  finaglePartitioning,
  finagleTunable,
  finagleIntegration,
  finagleExp,
  finagleGrpcContext,
  finagleOpenCensusTracing,
  finagleInit,
  finagleScribe,
  // Protocols
  finagleHttp,
  finagleBaseHttp,
  finagleHttp2,
  finagleThrift,
  finagleMemcached,
  finagleMux,
  finagleThriftMux,
  finagleMySQL,
  finagleRedis,
  finagleNetty4Http,
  finaglePostgresql
)

lazy val finagle = Project(
  id = "finagle",
  base = file(".")
).enablePlugins(
    ScalaUnidocPlugin
  ).settings(
    nettyVersionInfo := {
      val log = sLog.value
      log.info(s"Using Netty SNAPSHOT build mode: ${useNettySnapshot}")
      log.info(s"Netty version: ${netty4Version}")
      log.info(s"Netty tcnative version ${netty4StaticSslVersion}")
      ""
    },
    sharedSettings ++
      noPublishSettings ++
      Seq(
        ScalaUnidoc / unidoc / unidocProjectFilter :=
          inAnyProject -- inProjects(
            finagleBenchmark,
            finagleBenchmarkThrift,
            finagleExample
          ),
        // We don't generate javadoc for finagle-serversets, so exclude it from
        // unidoc.
        ScalaUnidoc / unidoc / unidocAllSources :=
          (ScalaUnidoc / unidoc / unidocAllSources).value.map(_.filterNot { file =>
            file.getPath.contains("finagle-serversets") &&
            file.getName.endsWith(".java")
          })
      )
  ).aggregate(projectList: _*)

lazy val finagleIntegration = Project(
  id = "finagle-integration",
  base = file("finagle-integration")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-integration",
    libraryDependencies ++= Seq(util("core")) ++ scroogeLibs
  ).dependsOn(
    finagleCore % "compile->compile;test->test",
    finagleHttp,
    finagleHttp2,
    finagleMySQL % "test->compile;test->test",
    finagleMemcached,
    finagleMux,
    finagleNetty4Http,
    finagleRedis % "test",
    finagleThrift,
    finagleThriftMux % "test->compile;test->test"
  )

lazy val finagleToggle = Project(
  id = "finagle-toggle",
  base = file("finagle-toggle")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-toggle",
    libraryDependencies ++= Seq(util("app"), util("core"), util("logging"), util("stats")) ++
      jacksonLibs
  )

lazy val finagleInit = Project(
  id = "finagle-init",
  base = file("finagle-init")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-init"
  )

lazy val finagleCore = Project(
  id = "finagle-core",
  base = file("finagle-core")
).settings(
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
      util("routing"),
      util("security"),
      util("stats"),
      "com.twitter" %% "util-test" % releaseVersion % "test" excludeAll (ExclusionRule(organization =
    "junit"),
  ExclusionRule(organization = "org.scala-tools.testing"),
  ExclusionRule(organization = "org.mockito")),
      util("tunable"),
      caffeineLib,
      hdrHistogramLib,
      jsr305Lib
    ) ++ netty4LibsTest,
    Test / unmanagedClasspath ++= (LocalProject("finagle-netty4") / Compile / fullClasspath).value
  ).dependsOn(finagleToggle, finagleInit)

lazy val finagleNetty4 = Project(
  id = "finagle-netty4",
  base = file("finagle-netty4")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-netty4",
    libraryDependencies ++= Seq(
      util("app"),
      util("cache"),
      util("codec"),
      util("core"),
      util("codec"),
      util("lint"),
      util("stats")
    ) ++ netty4Libs
  ).dependsOn(
    finagleCore % "compile->compile;test->test",
    finagleToggle
  )

lazy val finagleStatsCore = Project(
  id = "finagle-stats-core",
  base = file("finagle-stats-core")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-stats-core",
    libraryDependencies ++= Seq(
      util("app"),
      util("core"),
      util("lint"),
      util("logging"),
      util("registry"),
      util("stats"),
      util("tunable")
    ),
    libraryDependencies ++= jacksonLibs
  ).dependsOn(
    finagleCore,
    finagleHttp,
    finagleToggle,
    finagleTunable
  )

lazy val finagleStats = Project(
  id = "finagle-stats",
  base = file("finagle-stats")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-stats"
  ).dependsOn(
    finagleStatsCore
  )

lazy val finagleZipkinCore = Project(
  id = "finagle-zipkin-core",
  base = file("finagle-zipkin-core")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-zipkin-core",
    libraryDependencies ++= Seq(
      util("codec"),
      util("core"),
      util("stats")) ++ scroogeLibs ++ jacksonLibs
  ).dependsOn(finagleCore % "compile->compile;test->test", finagleThrift)

lazy val finagleZipkinScribe = Project(
  id = "finagle-zipkin-scribe",
  base = file("finagle-zipkin-scribe")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-zipkin-scribe",
    libraryDependencies ++= scroogeLibs
  ).dependsOn(finagleCore, finagleScribe, finagleThrift, finagleZipkinCore)

lazy val finagleServersets = Project(
  id = "finagle-serversets",
  base = file("finagle-serversets")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-serversets",
    libraryDependencies ++= Seq(
      caffeineLib,
      util("cache"),
      util("zk-test") % "test",
      "com.google.code.gson" % "gson" % "2.3.1",
      "org.apache.zookeeper" % "zookeeper" % zkVersion excludeAll (
        ExclusionRule("com.sun.jdmk", "jmxtools"),
        ExclusionRule("com.sun.jmx", "jmxri"),
        ExclusionRule("javax.jms", "jms")
      ),
      "commons-lang" % "commons-lang" % "2.6"
    ),
    libraryDependencies ++= jacksonLibs,
    libraryDependencies ++= scroogeLibs,
    Compile / ScroogeSBT.autoImport.scroogeLanguages := Seq("java"),
    unmanagedSources / excludeFilter := "ZkTest.scala",
    Compile / doc / scalacOptions ++= {
      if (scalaVersion.value.startsWith("2.12")) Seq("-no-java-comments")
      else Nil
    }
  ).dependsOn(finagleCore, finaglePartitioning)

lazy val finaglePartitioning = Project(
  id = "finagle-partitioning",
  base = file("finagle-partitioning")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-partitioning",
    libraryDependencies ++= Seq(
      util("core"),
      util("hashing"),
      scalaCollectionCompat
    )
  ).dependsOn(
    finagleNetty4,
    finagleCore % "compile->compile;test->test"
  )

lazy val finagleTunable = Project(
  id = "finagle-tunable",
  base = file("finagle-tunable")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-tunable",
    libraryDependencies ++= Seq(
      util("core"),
      util("tunable")
    ),
    libraryDependencies ++= jacksonLibs
  ).dependsOn(finagleToggle)

lazy val finagleScribe = Project(
  id = "finagle-scribe",
  base = file("finagle-scribe")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-scribe",
    libraryDependencies ++= Seq(
      util("core")
    ),
    libraryDependencies ++= scroogeLibs
  ).dependsOn(finagleCore, finagleThrift)

// Protocol support

lazy val finagleHttp = Project(
  id = "finagle-http",
  base = file("finagle-http")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-http",
    libraryDependencies ++= Seq(
      util("codec"),
      util("logging"),
      netty4StaticSsl
    )
  ).dependsOn(finagleBaseHttp, finagleNetty4Http, finagleHttp2, finagleToggle)

lazy val finagleBaseHttp = Project(
  id = "finagle-base-http",
  base = file("finagle-base-http")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-base-http",
    libraryDependencies ++= Seq(
      util("logging"),
      netty4Http
    ) ++ netty4Libs
  ).dependsOn(finagleCore, finagleToggle)

lazy val finagleLogging = Project(
  id = "finagle-logging",
  base = file("finagle-logging")
).settings(
  sharedSettings
).settings(
  name := "finagle-logging",
  libraryDependencies ++= Seq(
    hdrHistogramLib,
    util("core"),
    util("slf4j-api"),
  )
).dependsOn(finagleCore)

lazy val finagleNetty4Http = Project(
  id = "finagle-netty4-http",
  base = file("finagle-netty4-http")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-netty4-http",
    libraryDependencies ++= Seq(
      util("app"),
      util("codec"),
      util("core"),
      util("jvm"),
      util("stats"),
      netty4Http
    )
  ).dependsOn(finagleNetty4, finagleBaseHttp % "test->test;compile->compile")

lazy val finagleHttp2 = Project(
  id = "finagle-http2",
  base = file("finagle-http2")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-http2",
    libraryDependencies ++= Seq(
      netty4Http,
      netty4Http2,
      netty4StaticSsl,
      util("cache"),
      util("core"),
      util("logging")
    ) ++ netty4Libs
  ).dependsOn(finagleCore, finagleNetty4, finagleNetty4Http, finagleBaseHttp)

lazy val finagleThrift = Project(
  id = "finagle-thrift",
  base = file("finagle-thrift")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-thrift",
    libraryDependencies ++= scroogeLibs
  ).dependsOn(finagleCore, finagleNetty4, finaglePartitioning, finagleToggle)

lazy val finagleMemcached = Project(
  id = "finagle-memcached",
  base = file("finagle-memcached")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-memcached",
    libraryDependencies ++= Seq(
      util("hashing"),
      util("zk-test") % "test",
      "com.twitter" %% "bijection-core" % "0.9.7",
      "org.apache.thrift" % "libthrift" % libthriftVersion
    ),
    libraryDependencies ++= jacksonLibs
  ).dependsOn(
    // NOTE: Order is important here.
    // finagleNetty4 must come before finagleCore here, otherwise
    // tests will fail with NoClassDefFound errors due to
    // StringClient and StringServer.
    finagleNetty4,
    finagleCore % "compile->compile;test->test",
    finagleServersets,
    finaglePartitioning,
    finagleStats,
    finagleToggle
  )

lazy val finagleRedis = Project(
  id = "finagle-redis",
  base = file("finagle-redis")
).settings(
    sharedSettings
  ).configs(
    IntegrationTest extend (Test)
  ).settings(
    Defaults.itSettings: _*
  ).settings(
    name := "finagle-redis",
    libraryDependencies ++= Seq(
      util("logging")
    )
  ).dependsOn(finagleCore, finagleNetty4, finaglePartitioning)

lazy val finagleMux = Project(
  id = "finagle-mux",
  base = file("finagle-mux")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-mux",
    libraryDependencies ++= Seq(
      util("app"),
      util("core"),
      util("logging"),
      util("stats"),
      lz4Lib % "test"
    )
  ).dependsOn(finagleCore % "compile->compile;test->test", finagleNetty4, finagleToggle)

lazy val finagleThriftMux = Project(
  id = "finagle-thriftmux",
  base = file("finagle-thriftmux")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-thriftmux",
    libraryDependencies ++= Seq(util("core"), util("logging"), util("stats")) ++ scroogeLibs
  ).dependsOn(
    finagleCore % "compile->compile;test->test",
    finagleMux,
    finagleThrift % "compile->compile;test->test")

lazy val finagleMySQL = Project(
  id = "finagle-mysql",
  base = file("finagle-mysql")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-mysql",
    libraryDependencies ++= Seq(
      util("logging"),
      util("cache"),
      util("core"),
      util("stats"),
      caffeineLib,
      jsr305Lib
    ) ++ jacksonLibs,
    unmanagedSources / excludeFilter := {
      "EmbeddableMysql.scala" || "ClientTest.scala"
    }
  ).dependsOn(finagleCore, finagleNetty4, finagleToggle)

lazy val finaglePostgresql = Project(
  id = "finagle-postgresql",
  base = file("finagle-postgresql")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-postgresql",
    libraryDependencies ++= Seq(
      util("core"),
      util("stats")
    )
  ).dependsOn(finagleCore, finagleNetty4)

lazy val finagleExp = Project(
  id = "finagle-exp",
  base = file("finagle-exp")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-exp",
    libraryDependencies ++= Seq(
      "com.netflix.concurrency-limits" % "concurrency-limits-core" % "0.3.0"
    )
  ).dependsOn(
    finagleCore % "compile->compile;test->test",
    finagleNetty4 % "test"
  )

lazy val finagleGrpcContext = Project(
  id = "finagle-grpc-context",
  base = file("finagle-grpc-context")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-grpc-context",
    libraryDependencies ++= Seq(
      util("core"),
      "io.grpc" % "grpc-context" % "1.13.2"
    )
  )

lazy val finagleOpenCensusTracing = Project(
  id = "finagle-opencensus-tracing",
  base = file("finagle-opencensus-tracing")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-opencensus-tracing",
    libraryDependencies ++= Seq(
      "io.opencensus" % "opencensus-api" % opencensusVersion,
      "io.opencensus" % "opencensus-impl" % opencensusVersion,
      "io.opencensus" % "opencensus-contrib-http-util" % opencensusVersion % "test"
    ) ++ scroogeLibs
  ).dependsOn(
    finagleCore,
    finagleHttp,
    finagleGrpcContext,
    // needs the thruftmux test dependency for testing
    finagleThriftMux % "compile->compile;test->test"
  )

lazy val finagleExample = Project(
  id = "finagle-example",
  base = file("finagle-example")
).settings(
    sharedSettings
  ).settings(
    name := "finagle-example",
    libraryDependencies ++= Seq(
      util("codec"),
      "org.slf4j" % "slf4j-nop" % "1.7.30" % "provided"
    ) ++ scroogeLibs
  ).dependsOn(
    finagleCore,
    finagleHttp,
    finagleMemcached,
    finagleMySQL,
    finagleRedis,
    finagleStats,
    finagleThrift)

lazy val finagleBenchmarkThrift = Project(
  id = "finagle-benchmark-thrift",
  base = file("finagle-benchmark-thrift")
).settings(
    sharedSettings
  ).settings(
    libraryDependencies ++= scroogeLibs
  ).dependsOn(finagleThrift)

lazy val finagleBenchmark = Project(
  id = "finagle-benchmark",
  base = file("finagle-benchmark")
).settings(
    sharedSettings
  ).enablePlugins(
    JmhPlugin
  ).settings(
    name := "finagle-benchmark",
    libraryDependencies ++= Seq(
      util("codec"),
      "org.apache.curator" % "curator-test" % "2.8.0",
      "org.apache.curator" % "curator-framework" % "2.8.0"
    )
  ).dependsOn(
    finagleBenchmarkThrift,
    finagleCore,
    finagleExp,
    finagleHttp,
    finagleMemcached,
    finagleMux,
    finagleMySQL,
    finagleNetty4,
    finaglePostgresql,
    finagleStats,
    finagleThriftMux,
    finagleZipkinScribe
  ).aggregate(finagleBenchmarkThrift)

lazy val finagleDoc = Project(
  id = "finagle-doc",
  base = file("doc")
).enablePlugins(
    SphinxPlugin
  ).settings(
    sharedSettings
  ).settings(
    doc / scalacOptions ++= Seq("-doc-title", "Finagle", "-doc-version", version.value),
    Sphinx / includeFilter := ("*.html" | "*.png" | "*.svg" | "*.js" | "*.css" | "*.gif" | "*.txt"),
    // Workaround for sbt bug: Without a testGrouping for all test configs,
    // the wrong tests are run
    testGrouping := (Test / definedTests map partitionTests).value,
    DocTest / testGrouping := (DocTest / definedTests map partitionTests).value
  ).configs(
    DocTest
  ).settings(
    inConfig(DocTest)(Defaults.testSettings): _*
  ).settings(
    DocTest / unmanagedSourceDirectories += baseDirectory.value / "src/sphinx/code",
    DocTest / //resourceDirectory <<= baseDirectory { _ / "src/test/resources" }

    // Make the "test" command run both, test and doctest:test
    test := Seq(Test / test, DocTest / test).dependOn.value
  ).dependsOn(finagleCore, finagleHttp, finagleMySQL)

/* Test Configuration for running tests on doc sources */
lazy val DocTest = config("doctest") extend Test

// A dummy partitioning scheme for tests
def partitionTests(tests: Seq[TestDefinition]) = {
  Seq(new Group("inProcess", tests, InProcess))
}

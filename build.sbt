import Tests._
import scoverage.ScoverageKeys

// All Twitter library releases are date versioned as YY.MM.patch
val releaseVersion = "18.6.0-SNAPSHOT"

val libthriftVersion = "0.10.0"

val netty4Version = "4.1.16.Final"

// zkVersion should be kept in sync with the 'util-zk' dependency version
val zkVersion = "3.5.0-alpha"

val caffeineLib = "com.github.ben-manes.caffeine" % "caffeine" % "2.3.4"
val hdrHistogramLib = "org.hdrhistogram" % "HdrHistogram" % "2.1.10"
val jsr305Lib = "com.google.code.findbugs" % "jsr305" % "2.0.1"
val netty3Lib = "io.netty" % "netty" % "3.10.1.Final"
val netty4Libs = Seq(
  "io.netty" % "netty-handler" % netty4Version,
  "io.netty" % "netty-transport" % netty4Version,
  "io.netty" % "netty-transport-native-epoll" % netty4Version classifier "linux-x86_64",
  // this package is a dep of native-epoll above, explicitly add this for coursier plugin
  "io.netty" % "netty-transport-native-unix-common" % netty4Version,
  "io.netty" % "netty-handler-proxy" % netty4Version
)
val netty4LibsTest = Seq(
  "io.netty" % "netty-handler" % netty4Version % "test",
  "io.netty" % "netty-transport" % netty4Version % "test",
  "io.netty" % "netty-transport-native-epoll" % netty4Version % "test" classifier "linux-x86_64",
  // this package is a dep of native-epoll above, explicitly add this for coursier plugin
  "io.netty" % "netty-transport-native-unix-common" % netty4Version % "test",
  "io.netty" % "netty-handler-proxy" % netty4Version % "test"
)
val netty4Http = "io.netty" % "netty-codec-http" % netty4Version
val netty4Http2 = "io.netty" % "netty-codec-http2" % netty4Version
val netty4StaticSsl = "io.netty" % "netty-tcnative-boringssl-static" % "2.0.6.Final"
val jacksonVersion = "2.8.4"
val jacksonLibs = Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion exclude("com.google.guava", "guava")
)
val thriftLibs = Seq(
  "org.apache.thrift" % "libthrift" % libthriftVersion intransitive()
)
val scroogeLibs = thriftLibs ++ Seq(
  "com.twitter" %% "scrooge-core" % releaseVersion)

def util(which: String) =
  "com.twitter" %% ("util-"+ which) % releaseVersion excludeAll(
    ExclusionRule(organization = "junit"),
    ExclusionRule(organization = "org.scala-tools.testing"),
    ExclusionRule(organization = "org.mockito"))

val sharedSettings = Seq(
  version := releaseVersion,
  organization := "com.twitter",
  scalaVersion := "2.12.4",
  crossScalaVersions := Seq("2.11.11", "2.12.4"),
  libraryDependencies ++= Seq(
    "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
    "org.scalatest" %% "scalatest" % "3.0.0" % "test",
    // See http://www.scala-sbt.org/0.13/docs/Testing.html#JUnit
    "com.novocode" % "junit-interface" % "0.11" % "test",
    "org.mockito" % "mockito-all" % "1.9.5" % "test"
  ),

  ScoverageKeys.coverageHighlighting := true,
  ScroogeSBT.autoImport.scroogeLanguages in Test := Seq("java", "scala"),

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

  javaOptions in Test := Seq("-DSKIP_FLAKY=true"),

  // This is bad news for things like com.twitter.util.Time
  parallelExecution in Test := false,

  // -a: print stack traces for failing asserts
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-a"),

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
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },

  // Prevent eviction warnings
  dependencyOverrides ++= (scalaVersion { vsn =>
    Seq(
      "org.apache.thrift" % "libthrift" % libthriftVersion
    )
  }).value,

  resourceGenerators in Compile += Def.task {
    val dir = (resourceManaged in Compile).value
    val file = dir / "com" / "twitter" / name.value / "build.properties"
    val buildRev = scala.sys.process.Process("git" :: "rev-parse" :: "HEAD" :: Nil).!!.trim
    val buildName = new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(new java.util.Date)
    val contents = s"name=${name.value}\nversion=${version.value}\nbuild_revision=$buildRev\nbuild_name=$buildName"
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
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  // sbt-pgp's publishSigned task needs this defined even though it is not publishing.
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo")))
)

lazy val projectList = Seq[sbt.ProjectReference](
  // Core, support.
  finagleToggle,
  finagleCore,
  finagleNetty4,
  finagleNetty3,
  finagleStats,
  finagleZipkinCore,
  finagleZipkinScribe,
  finagleServersets,
  finagleTunable,
  finagleException,
  finagleIntegration,
  finagleExp,
  finagleInit,

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
  finagleHttpCookie
)

lazy val finagle = Project(
  id = "finagle",
  base = file(".")
).enablePlugins(
  ScalaUnidocPlugin
).settings(
  sharedSettings ++
  noPublishSettings ++
  Seq(
    unidocProjectFilter in(ScalaUnidoc, unidoc) :=
      inAnyProject -- inProjects(
        finagleBenchmark,
        finagleBenchmarkThrift,
        finagleExample
      ),
    // We don't generate javadoc for finagle-serversets, so exclude it from
    // unidoc.
    unidocAllSources in(ScalaUnidoc, unidoc) :=
      (unidocAllSources in(ScalaUnidoc, unidoc)).value.map(_.filterNot { file =>
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
  libraryDependencies ++= Seq(
    util("app"),
    util("core"),
    util("logging"),
    util("stats")) ++
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
    util("security"),
    util("stats"),
    util("tunable"),
    caffeineLib,
    hdrHistogramLib,
    jsr305Lib
  ) ++ netty4LibsTest,
  unmanagedClasspath in Test ++= (fullClasspath in (LocalProject("finagle-netty4"), Compile)).value
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

lazy val finagleNetty3 = Project(
  id = "finagle-netty3",
  base = file("finagle-netty3")
).settings(
  sharedSettings
).settings(
  name := "finagle-netty3",
  libraryDependencies ++= Seq(
    util("app"),
    util("cache"),
    util("codec"),
    util("core"),
    util("codec"),
    util("lint"),
    util("stats"),
    netty3Lib
  )
).dependsOn(finagleCore)

lazy val finagleStats = Project(
  id = "finagle-stats",
  base = file("finagle-stats")
).settings(
  sharedSettings
).settings(
  name := "finagle-stats",
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
).dependsOn(finagleCore, finagleThrift)

lazy val finagleZipkinScribe = Project(
  id = "finagle-zipkin-scribe",
  base = file("finagle-zipkin-scribe")
).settings(
  sharedSettings
).settings(
  name := "finagle-zipkin-scribe",
  libraryDependencies ++= scroogeLibs
).dependsOn(finagleCore, finagleThrift, finagleZipkinCore)

lazy val finagleException = Project(
  id = "finagle-exception",
  base = file("finagle-exception")
).settings(
  sharedSettings
).settings(
  name := "finagle-exception",
  libraryDependencies ++= Seq(
    util("codec")
  ) ++ scroogeLibs,
  libraryDependencies ++= jacksonLibs
).dependsOn(finagleCore, finagleThrift)

lazy val finagleServersets = Project(
  id = "finagle-serversets",
  base = file("finagle-serversets")
).settings(
  sharedSettings
).settings(
  name := "finagle-serversets",
  fork in Test := true,
  libraryDependencies ++= Seq(
    caffeineLib,
    util("cache"),
    util("zk-test") % "test",
    "com.google.code.gson" % "gson" % "2.3.1",
    "org.apache.zookeeper" % "zookeeper" % zkVersion excludeAll(
      ExclusionRule("com.sun.jdmk", "jmxtools"),
      ExclusionRule("com.sun.jmx", "jmxri"),
      ExclusionRule("javax.jms", "jms")
    ),
    "commons-lang" % "commons-lang" % "2.6"
  ),
  libraryDependencies ++= jacksonLibs,
  libraryDependencies ++= scroogeLibs,
  ScroogeSBT.autoImport.scroogeLanguages in Compile := Seq("java"),
  excludeFilter in unmanagedSources := "ZkTest.scala",
  scalacOptions in (Compile, doc) ++= {
    if (scalaVersion.value.startsWith("2.12")) Seq("-no-java-comments")
    else Nil
  }
).dependsOn(finagleCore)

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
    util("collection"),
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
    util("collection"),
    util("logging"),
    netty4Http
  ) ++ netty4Libs
).dependsOn(finagleCore, finagleNetty3, finagleToggle, finagleHttpCookie)

lazy val finagleNetty4Http = Project(
  id = "finagle-netty4-http",
  base = file("finagle-netty4-http")
).settings(
  sharedSettings
).settings(
  name := "finagle-netty4-http",
  libraryDependencies ++= Seq(
    util("app"), util("codec"), util("core"), util("jvm"), util("stats"),
    "commons-lang" % "commons-lang" % "2.6",
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
  libraryDependencies ++=
    Seq(
      "commons-lang" % "commons-lang" % "2.6" % "test") ++ scroogeLibs
).dependsOn(finagleCore, finagleNetty4, finagleToggle)

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
    "com.twitter" %% "bijection-core" % "0.9.4",
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
  finagleStats,
  finagleToggle
)

lazy val finagleRedis = Project(
  id = "finagle-redis",
  base = file("finagle-redis")
).settings(
  sharedSettings
).configs(
  IntegrationTest extend(Test)
).settings(
  Defaults.itSettings: _*
).settings(
  name := "finagle-redis",
  libraryDependencies ++= Seq(
    util("logging")
  )
).dependsOn(finagleCore, finagleNetty4)

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
    util("stats"))
).dependsOn(
  finagleCore % "compile->compile;test->test",
  finagleNetty4,
  finagleToggle)

lazy val finagleThriftMux = Project(
  id = "finagle-thriftmux",
  base = file("finagle-thriftmux")
).settings(
  sharedSettings
).settings(
  name := "finagle-thriftmux",
  libraryDependencies ++= Seq(
    "commons-lang" % "commons-lang" % "2.6",
    util("core"),
    util("logging"),
    util("stats")) ++ scroogeLibs
).dependsOn(
  finagleCore % "compile->compile;test->test",
  finagleMux,
  finagleThrift)

lazy val finagleMySQL = Project(
  id = "finagle-mysql",
  base = file("finagle-mysql")
).settings(
  sharedSettings
).settings(
  name := "finagle-mysql",
  libraryDependencies ++= Seq(util("logging"), util("cache"), caffeineLib, jsr305Lib),
  excludeFilter in unmanagedSources := {
    "EmbeddableMysql.scala" || "ClientTest.scala"
  }
).dependsOn(finagleCore, finagleNetty4, finagleToggle)

lazy val finagleExp = Project(
  id = "finagle-exp",
  base = file("finagle-exp")
).settings(
  sharedSettings
).settings(
  name := "finagle-exp"
).dependsOn(finagleCore, finagleThrift)

lazy val finagleExample = Project(
  id = "finagle-example",
  base = file("finagle-example")
).settings(
  sharedSettings
).settings(
  name := "finagle-example",
  libraryDependencies ++= Seq(
    util("codec"),
    "org.slf4j" % "slf4j-nop" % "1.7.7" % "provided"
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
  finagleStats,
  finagleThriftMux,
  finagleZipkinScribe
).aggregate(finagleBenchmarkThrift)

lazy val finagleHttpCookie = Project(
  id = "finagle-http-cookie",
  base = file("finagle-http-cookie")
).settings(
  sharedSettings
).settings(
  name := "finagle-http-cookie",
  libraryDependencies ++= Seq(
    netty3Lib
  )
)

lazy val finagleDoc = Project(
  id = "finagle-doc",
  base = file("doc")
).enablePlugins(
  SphinxPlugin
).settings(
  sharedSettings
).settings(
  scalacOptions in doc ++= Seq("-doc-title", "Finagle", "-doc-version", version.value),
  includeFilter in Sphinx := ("*.html" | "*.png" | "*.svg" | "*.js" | "*.css" | "*.gif" | "*.txt"),

  // Workaround for sbt bug: Without a testGrouping for all test configs,
  // the wrong tests are run
  testGrouping := (definedTests in Test map partitionTests).value,
  testGrouping in DocTest := (definedTests in DocTest map partitionTests).value
).configs(
  DocTest
).settings(
  inConfig(DocTest)(Defaults.testSettings): _*
).settings(
  unmanagedSourceDirectories in DocTest += baseDirectory.value / "src/sphinx/code",
  //resourceDirectory in DocTest <<= baseDirectory { _ / "src/test/resources" }

  // Make the "test" command run both, test and doctest:test
  test := Seq(test in Test, test in DocTest).dependOn.value
).dependsOn(finagleCore, finagleHttp, finagleMySQL)

/* Test Configuration for running tests on doc sources */
lazy val DocTest = config("doctest") extend Test

// A dummy partitioning scheme for tests
def partitionTests(tests: Seq[TestDefinition]) = {
  Seq(new Group("inProcess", tests, InProcess))
}

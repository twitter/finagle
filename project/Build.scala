import sbt._
import Keys._
import com.twitter.sbt._

object Finagle extends Build {
  val zkVersion = "3.3.4"
  val utilVersion = "6.0.1"
  val nettyLib = "io.netty" % "netty" % "3.5.5.Final" withSources()
  val ostrichLib = "com.twitter" % "ostrich" % "9.0.2" withSources()
  val thriftLibs = Seq(
    "org.apache.thrift" % "libthrift" % "0.5.0" intransitive(),
    "org.slf4j"   % "slf4j-nop" % "1.5.8" % "provided"
  )

  def util(which: String) = "com.twitter" % ("util-"+which) % utilVersion withSources()

  val sharedSettings = Seq(
    version := "5.3.8",
    organization := "com.twitter",
    scalaVersion := "2.9.2",
    SubversionPublisher.subversionRepository := Some("https://svn.twitter.biz/maven-public"),
    libraryDependencies ++= Seq(
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test" withSources(),
      "junit" % "junit" % "4.8.1" % "test" withSources(),
      "org.mockito" % "mockito-all" % "1.8.5" % "test" withSources()
    ),
    resolvers += "twitter-repo" at "http://maven.twttr.com",
    resolvers += "Coda Hale's Repository" at "http://repo.codahale.com/",

    ivyXML :=
      <dependencies>
        <exclude org="com.sun.jmx" module="jmxri" />
        <exclude org="com.sun.jdmk" module="jmxtools" />
        <exclude org="javax.jms" module="jms" />
      </dependencies>,

    scalacOptions ++= Seq("-encoding", "utf8"),
    scalacOptions += "-deprecation",

    // This is bad news for things like com.twitter.util.Time
    parallelExecution in Test := false,

    // This effectively disables packageDoc, which craps out
    // on generating docs for generated thrift due to the use
    // of raw java types.
    packageDoc in Compile := new java.io.File("nosuchjar")
  )

  val jmockSettings = Seq(
    libraryDependencies ++= Seq(
      "org.jmock" % "jmock" % "2.4.0" % "test",
      "cglib" % "cglib" % "2.1_3" % "test",
      "asm" % "asm" % "1.5.3" % "test",
      "org.objenesis" % "objenesis" % "1.1" % "test",
      "org.hamcrest" % "hamcrest-all" % "1.1" % "test"
    )
  )

  lazy val finagle = Project(
    id = "finagle",
    base = file(".")
  ) aggregate(
    // Core, support.
    finagleCore, finagleTest, finagleOstrich4,
    finagleZipkin, finagleServersets,
    finagleException, finagleCommonsStats,

    // Protocols
    finagleHttp, finagleStream, finagleNative, finagleThrift,
    finagleMemcached, finagleKestrel,

    // Use and integration
    finagleStress, finagleExample, finagleBenchmark
  )

  lazy val finagleTest = Project(
    id = "finagle-test",
    base = file("finagle-test"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-test",
    libraryDependencies ++= Seq(nettyLib, util("core"))
  )

  lazy val finagleCore = Project(
    id = "finagle-core",
    base = file("finagle-core"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-core",
    libraryDependencies ++= Seq(nettyLib, util("core"), util("collection"), util("hashing"),
      util("jvm"), util("logging"))
  ).dependsOn(finagleTest % "test")

  lazy val finagleOstrich4 = Project(
    id = "finagle-ostrich4",
    base = file("finagle-ostrich4"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-ostrich4",
    libraryDependencies ++= Seq(ostrichLib)
  ).dependsOn(finagleCore)

  lazy val finagleZipkin = Project(
    id = "finagle-zipkin",
    base = file("finagle-zipkin"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      CompileThriftFinagle.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-zipkin",
    compileOrder := CompileOrder.JavaThenScala,
    libraryDependencies ++= Seq(util("codec")) ++ thriftLibs
  ).dependsOn(finagleCore, finagleThrift, finagleTest % "test")

  lazy val finagleException = Project(
    id = "finagle-exception",
    base = file("finagle-exception"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      CompileThriftFinagle.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-exception",
    libraryDependencies ++= Seq(
      util("codec"),
      "com.codahale" % "jerkson_2.8.1" % "0.1.4",
      "org.codehaus.jackson" % "jackson-core-asl"  % "1.8.1",
      "org.codehaus.jackson" % "jackson-mapper-asl" % "1.8.1",
      "com.twitter" % "streamyj_2.8.1" % "0.3.0" % "test"
    ) ++ thriftLibs
  ).dependsOn(finagleCore, finagleThrift)

  lazy val finagleCommonsStats = Project(
    id = "finagle-commons-stats",
    base = file("finagle-commons-stats"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-commons-stats",
    compileOrder := CompileOrder.JavaThenScala,
    libraryDependencies ++= Seq("com.twitter.common" % "stats" % "0.0.35")
  ).dependsOn(finagleCore)

  lazy val finagleServersets = Project(
    id = "finagle-serversets",
    base = file("finagle-serversets"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-serversets",
    libraryDependencies ++= Seq(
      "commons-codec" % "commons-codec" % "1.5",
      "com.twitter.common.zookeeper" % "client" % "0.0.6",
      "com.twitter.common.zookeeper" % "group" % "0.0.5"
    ),
    ivyXML :=
      <dependencies>
        <dependency org="com.twitter.common.zookeeper" name="server-set" rev="1.0.10">
          <exclude org="com.google.guava" name="guava"/>
          <exclude org="com.twitter" name="finagle-core"/>
          <exclude org="com.twitter" name="finagle-thrift"/>
          <exclude org="com.twitter" name="util-core"/>
          <exclude org="com.twitter.common" name="jdk-logging"/>
          <exclude org="com.twitter.common" name="stats"/>
          <exclude org="com.twitter.common" name="util-executor-service-shutdown"/>
          <exclude org="io.netty" name="netty"/>
          <exclude org="javax.activation" name="activation"/>
          <exclude org="javax.mail" name="mail"/>
        </dependency>
      </dependencies>
  ).dependsOn(finagleCore)

  // Protocol support

  lazy val finagleHttp = Project(
    id = "finagle-http",
    base = file("finagle-http"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-http",
    libraryDependencies ++= Seq(
      util("codec"), util("logging"),
      "commons-lang" % "commons-lang" % "2.6" withSources()
    )
  ).dependsOn(finagleCore)

  lazy val finagleNative = Project(
    id = "finagle-native",
    base = file("finagle-native"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-native"
  ).dependsOn(finagleCore, finagleHttp)

  lazy val finagleStream = Project(
    id = "finagle-stream",
    base = file("finagle-stream"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-stream"
  ).dependsOn(finagleCore, finagleKestrel, finagleTest % "test")

  lazy val finagleThrift = Project(
    id = "finagle-thrift",
    base = file("finagle-thrift"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      CompileThriftFinagle.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-thrift",
    compileOrder := CompileOrder.JavaThenScala,
    libraryDependencies ++= Seq("silly" % "silly-thrift" % "0.5.0" % "test") ++ thriftLibs
  ).dependsOn(finagleCore, finagleTest % "test")

  lazy val finagleMemcached = Project(
    id = "finagle-memcached",
    base = file("finagle-memcached"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-memcached",
    libraryDependencies ++= Seq(
      util("hashing"),
      "com.google.guava" % "guava" % "11.0.2",
      "com.twitter.common" % "zookeeper-testing" % "0.0.20" % "test"
    )
  ).dependsOn(finagleCore, finagleServersets)

  lazy val finagleKestrel = Project(
    id = "finagle-kestrel",
    base = file("finagle-kestrel"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-kestrel"
  ).dependsOn(finagleCore, finagleMemcached)

/*  notyet
  lazy val finagleProtobuf = Project(
    id = "finagle-protobuf",
    base = file("finagle-protobuf"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-protobuf",
    libraryDependencies ++= Seq(
      "junit" % "junit" % "4.10",
      "com.google.protobuf" % "protobuf-java" % "2.4.1",
      "org.slf4j" % "slf4j-nop" % "1.5.8" % "provided"
    )
  ).dependsOn(finagleCore)
*/

  lazy val finagleRedis = Project(
    id = "finagle-redis",
    base = file("finagle-redis"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-redis",
    libraryDependencies ++= Seq(
      util("logging")
    ),
    testOptions in Test := Seq(Tests.Filter {
      case "com.twitter.finagle.redis.protocol.integration.ClientServerIntegrationSpec" => false
      case "com.twitter.finagle.redis.integration.ClientSpec" => false
      case _ => true
    })
  ).dependsOn(finagleCore)

  // Uses

  lazy val finagleStress = Project(
    id = "finagle-stress",
    base = file("finagle-stress"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      CompileThriftFinagle.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-stress",
    libraryDependencies ++= Seq(ostrichLib, util("logging")) ++ thriftLibs
  ).dependsOn(finagleCore, finagleOstrich4, finagleThrift, finagleHttp)

  lazy val finagleExample = Project(
    id = "finagle-example",
    base = file("finagle-example"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      CompileThriftFinagle.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-example",
    libraryDependencies ++= Seq(
      util("codec"),
      "com.twitter.common" % "flags" % "0.0.1",
      "org.slf4j" %  "slf4j-nop" % "1.5.8" % "provided"
    )
  ).dependsOn(
    finagleCore, finagleHttp, finagleStream, finagleThrift,
    finagleMemcached, finagleKestrel, finagleRedis, finagleOstrich4)

  lazy val finagleBenchmark = Project(
    id = "finagle-benchmark",
    base = file("finagle-benchmark"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      CompileThriftFinagle.newSettings ++
      sharedSettings
  ).settings(
    name := "finagle-benchmark",
    libraryDependencies ++= Seq(
      util("codec"),
      "com.google.caliper" % "caliper" % "0.5-rc1"
    )
  ).dependsOn(finagleCore)

  lazy val finagleMySQL = Project(
    id = "finagle-mysql",
    base = file("finagle-mysql"),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      sharedSettings
    ).settings(
      name := "finagle-mysql",
      libraryDependencies ++= Seq(util("logging"))
    ).dependsOn(finagleCore)
}

# the stringprotocol target doesn't have an sbt analog so
# don't depend on it in projects which need to support sbt.
scala_library(
    name = "stringprotocol",
    sources = [
        "./com/twitter/finagle/client/StringClient.scala",
        "./com/twitter/finagle/server/StringServer.scala",
    ],
    fatal_warnings = True,
    dependencies = [
        "3rdparty/jvm/io/netty",
        "finagle/finagle-core",
        "finagle/finagle-netty4",
        "util/util-core",
        "util/util-stats",
    ],
)

scala_library(
    name = "pushsession-utils",
    sources = [
        "./com/twitter/finagle/exp/pushsession/DeferredExecutor.scala",
        "./com/twitter/finagle/exp/pushsession/MockChannelHandle.scala",
        "./com/twitter/finagle/exp/pushsession/MockPushSession.scala",
    ],
    fatal_warnings = True,
    dependencies = [
        "finagle/finagle-core/src/main/scala",
    ],
)

junit_tests(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    dependencies = [
        "3rdparty/jvm/com/github/ben-manes/caffeine",
        "3rdparty/jvm/io/netty",
        "3rdparty/jvm/io/netty:netty4-codec-http",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/hdrhistogram",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalacheck",
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-core/src/test/resources",
        "finagle/finagle-netty4",
        "finagle/finagle-toggle/src/main/scala",
        "util/util-app/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-hashing/src/main/scala",
        "util/util-jvm/src/main/scala",
        "util/util-lint/src/main/scala",
        "util/util-logging/src/main/scala",
        "util/util-registry/src/main/scala",
        "util/util-stats",
        "util/util-tunable/src/main/scala",
    ],
)

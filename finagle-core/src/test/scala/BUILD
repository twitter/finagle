# the stringprotocol target doesn't have an sbt analog so
# don't depend on it in projects which need to support sbt.
scala_library(
    name = "stringprotocol",
    sources = [
        "com/twitter/finagle/client/utils/StringClient.scala",
        "com/twitter/finagle/server/utils/StringServer.scala",
    ],
    compiler_option_sets = ["fatal_warnings"],
    tags = ["bazel-compatible"],
    dependencies = [
        "finagle/finagle-core",
        "finagle/finagle-netty4",
        "util/util-core",
        "util/util-stats",
    ],
)

scala_library(
    name = "pushsession-utils",
    sources = [
        "com/twitter/finagle/pushsession/utils/DeferredExecutor.scala",
        "com/twitter/finagle/pushsession/utils/MockChannelHandle.scala",
        "com/twitter/finagle/pushsession/utils/MockPushSession.scala",
    ],
    compiler_option_sets = ["fatal_warnings"],
    tags = ["bazel-compatible"],
    dependencies = [
        "finagle/finagle-core/src/main",
    ],
)

junit_tests(
    sources = [
        "!com/twitter/finagle/InitTest.scala",
        "**/*.scala",
    ],
    compiler_option_sets = ["fatal_warnings"],
    # env_local is tag for bazel only, added due to DPB-14188
    tags = ["bazel-compatible"],
    dependencies = [
        ":pushsession-utils",
        ":stringprotocol",
        "3rdparty/jvm/com/github/ben-manes/caffeine",
        "3rdparty/jvm/io/netty:netty4-codec-http",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/hdrhistogram",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalacheck",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:junit",
        "3rdparty/jvm/org/scalatestplus:mockito-1-10",
        "3rdparty/jvm/org/scalatestplus:scalacheck",
        "finagle/finagle-core/src/main",
        "finagle/finagle-core/src/test/resources",
        "finagle/finagle-netty4",
        "finagle/finagle-toggle/src/main/scala",
        "util/util-app/src/main/scala",
        "util/util-core:scala",
        "util/util-hashing/src/main/scala",
        "util/util-jvm/src/main/scala",
        "util/util-lint/src/main/scala",
        "util/util-logging/src/main/scala",
        "util/util-registry/src/main/scala",
        "util/util-routing/src/main/scala/com/twitter/util/routing",
        "util/util-security/src/main/scala/com/twitter/util/security",
        "util/util-stats",
        "util/util-tunable/src/main/scala",
    ],
)

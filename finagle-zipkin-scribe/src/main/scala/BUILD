scala_library(
    sources = ["**/*.scala"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-zipkin-scribe",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main",
        "finagle/finagle-scribe/src/main/scala/com/twitter/finagle/scribe",
        "finagle/finagle-thrift",
        "finagle/finagle-thrift/src/main/java",
        "finagle/finagle-thrift/src/main/thrift:scribe",
        "finagle/finagle-zipkin-core",
        "scrooge/scrooge-core/src/main/scala",
        "util/util-app",
        "util/util-core:scala",
        "util/util-stats/src/main/scala",
    ],
)

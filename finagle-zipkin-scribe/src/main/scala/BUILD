scala_library(
    sources = ["com/twitter/finagle/zipkin/thrift/ScribeZipkinTracer.scala"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-zipkin-scribe",
        repo = artifactory,
    ),
    # Exposes a service-loaded class.
    scope = "forced",
    dependencies = [
        ":core",
        "finagle/finagle-core/src/main",
        "finagle/finagle-zipkin-core/src/main/scala",
        "finagle/finagle-zipkin-scribe/src/main/resources",
    ],
    exports = [
        ":core",
    ],
)

scala_library(
    name = "core",
    sources = ["**/*.scala"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-zipkin-scribe-core",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main",
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

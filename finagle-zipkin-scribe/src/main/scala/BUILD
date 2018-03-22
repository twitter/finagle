scala_library(
    sources = rglobs("*.scala"),
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-zipkin-scribe",
        repo = artifactory,
    ),
    # Exposes a service-loaded class.
    scope = "forced",
    dependencies = [
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-thrift",
        "finagle/finagle-thrift/src/main/java",
        "finagle/finagle-zipkin-core",
        "finagle/finagle-zipkin-scribe/src/main/resources",
        "finagle/finagle-zipkin-scribe/src/main/thrift:thrift-scala",
        "scrooge/scrooge-core/src/main/scala",
        "util/util-app",
        "util/util-core/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
)

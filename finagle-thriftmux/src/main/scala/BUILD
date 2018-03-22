scala_library(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-thriftmux",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/io/netty:netty4",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-mux/src/main/scala",
        "finagle/finagle-thrift",
        "finagle/finagle-thrift/src/main/java",
        "finagle/finagle-thriftmux/src/main/resources",
        "finagle/finagle-toggle/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "finagle/finagle-mux/src/main/scala",
        "finagle/finagle-thrift",
    ],
)

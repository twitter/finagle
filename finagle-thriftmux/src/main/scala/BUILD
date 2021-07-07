scala_library(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-thriftmux",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/io/netty:netty4",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main",
        "finagle/finagle-mux/src/main/scala",
        "finagle/finagle-thrift",
        "finagle/finagle-thrift/src/main/java",
        "finagle/finagle-thriftmux/src/main/resources",
        "finagle/finagle-toggle/src/main/scala",
        "scrooge/scrooge-core/src/main/scala",
        "util/util-core:scala",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "finagle/finagle-mux/src/main/scala",
        "finagle/finagle-thrift",
    ],
)

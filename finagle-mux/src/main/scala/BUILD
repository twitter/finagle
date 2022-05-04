scala_library(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-mux",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/io/netty:netty4",
        "3rdparty/jvm/io/netty:netty4-codec",
        "3rdparty/jvm/io/netty:netty4-codec-http",
        "3rdparty/jvm/io/netty:netty4-handler-proxy",
        "finagle/finagle-core",
        "finagle/finagle-mux/src/main/resources",
        "finagle/finagle-netty4",
        "finagle/finagle-toggle/src/main/scala",
        "util/util-app/src/main/scala",
        "util/util-core:scala",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "3rdparty/jvm/io/netty:netty4",
        "finagle/finagle-core",
    ],
)

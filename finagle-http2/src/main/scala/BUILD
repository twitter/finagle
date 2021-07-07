scala_library(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-http2",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/io/netty:netty4",
        "3rdparty/jvm/io/netty:netty4-codec",
        "3rdparty/jvm/io/netty:netty4-codec-http",
        "3rdparty/jvm/io/netty:netty4-codec-http2",
        "3rdparty/jvm/io/netty:netty4-handler-proxy",
        scoped(
            "3rdparty/jvm/io/netty:netty4-tcnative-boringssl-static",
            scope = "runtime",
        ),
        "finagle/finagle-base-http",
        "finagle/finagle-core",
        "finagle/finagle-netty4",
        "finagle/finagle-netty4-http",
        "util/util-core",
        "util/util-logging",
        "util/util-stats",
    ],
)

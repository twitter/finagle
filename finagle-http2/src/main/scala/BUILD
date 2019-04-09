scala_library(
    sources = rglobs("*.scala"),
    compiler_option_sets = {"fatal_warnings"},
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-http2",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/io/netty:netty4",
        "3rdparty/jvm/io/netty:netty4-codec",
        "3rdparty/jvm/io/netty:netty4-codec-http",
        "3rdparty/jvm/io/netty:netty4-codec-http2",
        "3rdparty/jvm/io/netty:netty4-handler-proxy",
        "3rdparty/jvm/io/netty:netty4-tcnative-boringssl-static",
        "finagle/finagle-base-http",
        "finagle/finagle-core",
        "finagle/finagle-netty4",
        "finagle/finagle-netty4-http",
        "util/util-core",
        "util/util-logging",
        "util/util-stats",
    ],
)

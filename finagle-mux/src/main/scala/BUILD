scala_library(
    sources = rglobs("*.scala"),
    compiler_option_sets = {"fatal_warnings"},
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-mux",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/io/netty:netty4",
        "3rdparty/jvm/io/netty:netty4-codec-http",
        "3rdparty/jvm/io/netty:netty4-handler-proxy",
        "finagle/finagle-core",
        "finagle/finagle-netty4",
        "util/util-app/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "3rdparty/jvm/io/netty:netty4",
    ],
)

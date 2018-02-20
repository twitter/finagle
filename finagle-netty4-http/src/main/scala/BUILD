scala_library(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-netty4-http",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/io/netty",
        "3rdparty/jvm/io/netty:netty4",
        "3rdparty/jvm/io/netty:netty4-handler-proxy",
        "3rdparty/jvm/io/netty:netty4-http",
        "finagle/finagle-base-http",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-netty4",
        "finagle/finagle-netty4-http/src/main/resources",
        "util/util-core/src/main/scala",
        "util/util-logging/src/main/scala",
    ],
    exports = [
        "3rdparty/jvm/io/netty:netty4",
    ],
)

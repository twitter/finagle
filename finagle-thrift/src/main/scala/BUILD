scala_library(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-thrift",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/io/netty",
        "3rdparty/jvm/io/netty:netty4-handler-proxy",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-netty4",
        "finagle/finagle-thrift/src/main/java",
        "finagle/finagle-thrift/src/main/resources",
        "finagle/finagle-toggle/src/main/scala",
        "scrooge/scrooge-core",
        "util/util-app/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "3rdparty/jvm/io/netty:netty4-handler-proxy",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-thrift/src/main/java",
        "util/util-stats/src/main/scala",
    ],
)

# For usecases (like scrooge codegen) where this dependency is used, but not detected as
# used by zinc.
target(
    name = "forced",
    scope = "forced",
    dependencies = [":scala"],
)

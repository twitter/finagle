scala_library(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-thrift",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/io/netty:netty4-codec",
        "3rdparty/jvm/io/netty:netty4-handler-proxy",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main",
        "finagle/finagle-netty4",
        "finagle/finagle-partitioning/src/main/scala",
        "finagle/finagle-thrift/src/main/java",
        "finagle/finagle-thrift/src/main/resources",
        "finagle/finagle-toggle/src/main/scala",
        "scrooge/scrooge-core",
        "util/util-app/src/main/scala",
        "util/util-core:scala",
        "util/util-hashing",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "3rdparty/jvm/io/netty:netty4-handler-proxy",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main",
        "finagle/finagle-partitioning/src/main/scala",
        "finagle/finagle-thrift/src/main/java",
        "util/util-stats/src/main/scala",
    ],
)

# For usecases (like scrooge codegen) where this dependency is used, but not detected as
# used by zinc.
target(
    name = "forced",
    scope = "forced",
    tags = ["bazel-compatible"],
    dependencies = [":scala"],
)

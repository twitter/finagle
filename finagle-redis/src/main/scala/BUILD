scala_library(
    sources = ["**/*.scala"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-redis",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "finagle/finagle-core",
        "finagle/finagle-netty4",
        "finagle/finagle-partitioning",
        "util/util-core",
        "util/util-logging",
        "util/util-stats",
    ],
    exports = [
        "finagle/finagle-core",
        "finagle/finagle-partitioning",
        "util/util-core",
    ],
)

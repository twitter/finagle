scala_library(
    sources = ["*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-scribe",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "finagle/finagle-core",
        "finagle/finagle-thrift/src/main/scala",
        "finagle/finagle-thrift/src/main/thrift:scribe",
        "util/util-core:scala",
    ],
)

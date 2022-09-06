scala_library(
    sources = [
        "**/*.scala",
    ],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-logging",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/org/hdrhistogram",
        "finagle/finagle-core",
        "util/util-core",
        "util/util-slf4j-api",
    ],
    exports = [
        "util/util-core",
    ],
)

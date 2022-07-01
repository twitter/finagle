scala_library(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-server",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "util/util-core:scala",
        "util/util-registry/src/main/scala",
    ],
)

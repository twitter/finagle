java_library(
    sources = ["**/*.java"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = artifact(
        org = "com.twitter",
        name = "finagle-core-java",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "finagle/finagle-core/src/main",
        "util/util-core:scala",
        "util/util-core/src/main/java",
    ],
)

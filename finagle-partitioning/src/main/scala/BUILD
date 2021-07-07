scala_library(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-partitioning",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/org/scala-lang/modules:scala-collection-compat",
        "finagle/finagle-core",
        "util/util-core:scala",
        "util/util-hashing",
    ],
)

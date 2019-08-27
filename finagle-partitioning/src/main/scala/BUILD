scala_library(
    sources = rglobs("*.scala"),
    compiler_option_sets = {"fatal_warnings"},
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-partitioning",
        repo = artifactory,
    ),
    dependencies = [
        "finagle/finagle-core",
        "util/util-core/src/main/scala",
        "util/util-hashing",
    ],
)

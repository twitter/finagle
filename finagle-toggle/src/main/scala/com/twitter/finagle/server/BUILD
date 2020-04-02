scala_library(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-server",
        repo = artifactory,
    ),
    dependencies = [
        "util/util-core:scala",
        "util/util-registry/src/main/scala",
    ],
)

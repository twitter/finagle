scala_library(
    sources = rglobs("*.scala"),
    compiler_option_sets = {"fatal_warnings"},
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-partitioning",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/org/scala-lang/modules:scala-collection-compat",
        "finagle/finagle-core",
        "util/util-core/src/main/scala",
        "util/util-hashing",
    ],
)

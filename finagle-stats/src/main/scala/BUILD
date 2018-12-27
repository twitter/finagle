scala_library(
    sources = rglobs("*.scala"),
    compiler_option_sets = {"fatal_warnings"},
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-stats",
        repo = artifactory,
    ),
    # In the vast majority of cases, the consumers of finagle-stats only need it at runtime,
    # and so ideally we would mark this "scope=runtime". But there are enough consumers that also
    # need it at compiletime that it is non-trivial to split the target at this juncture.
    scope = "forced",
    dependencies = [
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-stats-core/src/main/scala",
        "finagle/finagle-stats/src/main/resources",
        "util/util-stats",
    ],
    exports = [
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-stats-core/src/main/scala",
        "util/util-stats",
    ],
)

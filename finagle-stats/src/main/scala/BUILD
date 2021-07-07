scala_library(
    # the reason we don't move this BUILD file to the stats directory and make this
    # an aggregator BUILD file is to avoid https://github.com/pantsbuild/pants/issues/6678
    sources = ["com/twitter/finagle/stats/stats.scala"],
    compiler_option_sets = ["fatal_warnings"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-stats",
        repo = artifactory,
    ),
    # In the vast majority of cases, the consumers of finagle-stats only need it at runtime,
    # and so ideally we would mark this "scope=runtime". But there are enough consumers that also
    # need it at compiletime that it is non-trivial to split the target at this juncture.
    scope = "forced",
    tags = ["bazel-compatible"],
    dependencies = [
        "finagle/finagle-core/src/main",
        "finagle/finagle-stats-core/src/main/scala",
        "finagle/finagle-stats/src/main/resources",
        "util/util-stats",
    ],
    exports = [
        "finagle/finagle-core/src/main",
        "finagle/finagle-stats-core/src/main/scala",
        "util/util-stats",
    ],
)

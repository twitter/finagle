scala_library(
    sources = rglobs("com/twitter/finagle/exp/*.scala"),
    fatal_warnings = True,
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-exp",
        repo = artifactory,
    ),
    dependencies = [
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-thrift/src/main/java",
        "util/util-core/src/main/scala",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "finagle/finagle-core/src/main/scala",
        "util/util-core/src/main/scala",
    ],
)

benchmark(
    name = "benchmark",
    sources = rglobs("com/twitter/finagle/benchmark/*.scala"),
    dependencies = [
        ":scala",
        "util/util-core",
    ],
)

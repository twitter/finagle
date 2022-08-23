scala_library(
    sources = ["com/twitter/finagle/exp/**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-exp",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/netflix/concurrency:concurrency-limits-core",
        "3rdparty/jvm/io/netty:netty4-resolver-dns",
        "finagle/finagle-core/src/main",
        "finagle/finagle-exp/src/main/resources/com/twitter/finagle/exp/fiber_scheduler",
        "finagle/finagle-netty4",
        "util/util-app/src/main/scala",
        "util/util-core:scala",
        "util/util-jvm",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
    exports = [
        "finagle/finagle-core/src/main",
        "util/util-core:scala",
    ],
)

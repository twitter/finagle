scala_library(
    sources = [
        "*.scala",
    ],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-mysql-tests-harness",
        repo = artifactory,
    ),
    strict_deps = True,
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/org/scalatestplus:junit",
        "finagle/finagle-core/src/main",
        "finagle/finagle-mysql",
        "finagle/finagle-mysql/src/test/scala/com/twitter/finagle/mysql/harness/config",
        "util/util-core:util-core-util",
        "util/util-logging",
        "util/util-stats/src/main/scala/com/twitter/finagle/stats",
    ],
)

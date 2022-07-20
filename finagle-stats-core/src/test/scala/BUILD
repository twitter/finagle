junit_tests(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/scalacheck",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:junit",
        "3rdparty/jvm/org/scalatestplus:scalacheck",
        "finagle/finagle-http/src/main/scala",
        "finagle/finagle-stats-core/src/main/scala",
        "finagle/finagle-stats-core/src/main/scala:filter_flags",
        "util/util-app/src/main/scala",
        "util/util-core:scala",
        "util/util-stats/src/main/scala",
    ],
)

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
        "finagle/finagle-core/src/main",
        "finagle/finagle-redis",
        "finagle/finagle-redis/src/test/scala",
        "util/util-core:scala",
        "util/util-logging/src/main/scala",
    ],
)

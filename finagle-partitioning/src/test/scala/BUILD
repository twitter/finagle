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
        "finagle/finagle-core/src/main",
        "finagle/finagle-core/src/test/scala:injectors-resolvers",
        "finagle/finagle-core/src/test/scala:stringprotocol",
        "finagle/finagle-partitioning",
        "util/util-core:scala",
        "util/util-logging/src/main/scala",
    ],
)

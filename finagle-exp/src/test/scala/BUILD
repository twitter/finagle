junit_tests(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/netflix/concurrency:concurrency-limits-core",
        "3rdparty/jvm/commons-io",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:junit",
        "finagle/finagle-core/src/main",
        "finagle/finagle-core/src/test/scala:injectors-resolvers",
        "finagle/finagle-core/src/test/scala:stringprotocol",
        "finagle/finagle-exp/src/main/scala",
        "finagle/finagle-exp/src/test/resources/com/twitter/finagle/exp/fiber_scheduler",
        "util/util-core:scala",
        "util/util-stats",
    ],
)

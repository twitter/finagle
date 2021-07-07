junit_tests(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/scalacheck",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:junit",
        "finagle/finagle-toggle/src/main/scala/com/twitter/finagle/server",
        "finagle/finagle-tunable/src/main/scala",
        "finagle/finagle-tunable/src/test/resources",
        "util/util-tunable/src/main/scala",
    ],
)

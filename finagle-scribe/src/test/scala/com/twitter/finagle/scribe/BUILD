junit_tests(
    sources = ["**/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:junit",
        "3rdparty/jvm/org/scalatestplus:mockito-1-10",
        "finagle/finagle-scribe/src/main/scala/com/twitter/finagle/scribe",
        "finagle/finagle-thrift/src/main/thrift:scribe",
        "util/util-core/src/main/scala/com/twitter/conversions",
        "util/util-stats/src/main/scala",
    ],
)

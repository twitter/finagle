junit_tests(
    sources = ["**/*.java"],
    compiler_option_sets = ["fatal_warnings"],
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/scalatestplus:junit",
        "finagle/finagle-base-http/src/main",
        "finagle/finagle-core/src/main",
        "finagle/finagle-core/src/main/java",
        "finagle/finagle-http",
        "finagle/finagle-http2",
        "util/util-core:scala",
        "util/util-core/src/main/java",
    ],
)

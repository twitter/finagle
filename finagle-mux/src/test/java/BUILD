junit_tests(
    sources = ["**/*.java"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/scalatestplus:junit",
        "finagle/finagle-core",
        "finagle/finagle-mux",
        "util/util-core",
    ],
)

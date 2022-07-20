junit_tests(
    sources = ["**/*.java"],
    compiler_option_sets = ["fatal_warnings"],
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/scala-lang:scala-library",
        "3rdparty/jvm/org/scalatestplus:junit",
        "finagle/finagle-core/src/main",
        "finagle/finagle-memcached",
        "finagle/finagle-memcached/src/test/scala:external",
        "util/util-core:scala",
        "util/util-core/src/main/java/com/twitter/io",
        "util/util-hashing/src/main/scala",
    ],
)

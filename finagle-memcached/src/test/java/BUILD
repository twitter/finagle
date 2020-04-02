junit_tests(
    sources = ["**/*.java"],
    compiler_option_sets = ["fatal_warnings"],
    dependencies = [
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/scala-lang:scala-library",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-memcached",
        "finagle/finagle-memcached/src/test/scala:external",
        "util/util-core:scala",
        "util/util-core/src/main/java/com/twitter/io",
        "util/util-hashing/src/main/scala",
    ],
)

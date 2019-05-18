junit_tests(
    sources = rglobs("*.scala"),
    compiler_option_sets = {"fatal_warnings"},
    dependencies = [
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/scalacheck",
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-redis",
        "finagle/finagle-redis/src/test/scala",
        "util/util-core/src/main/scala",
        "util/util-logging/src/main/scala",
    ],
)

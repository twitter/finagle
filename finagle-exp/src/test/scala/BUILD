junit_tests(
    sources = rglobs("*.scala"),
    compiler_option_sets = {"fatal_warnings"},
    dependencies = [
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-core/src/test/scala:stringprotocol",
        "finagle/finagle-exp",
        "finagle/finagle-thrift/src/main/java",
        "util/util-core/src/main/scala",
        "util/util-stats",
    ],
)

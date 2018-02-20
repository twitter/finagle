junit_tests(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind",
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-exception",
        "finagle/finagle-exception/src/main/thrift:thrift-scala",
        "util/util-codec",
        "util/util-core/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
)

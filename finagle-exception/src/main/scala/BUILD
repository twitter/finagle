scala_library(
    sources = rglobs("*.scala"),
    fatal_warnings = True,
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-exception",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind",
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-exception/src/main/thrift:thrift-scala",
        "finagle/finagle-thrift",
        "finagle/finagle-thrift/src/main/java",
        "scrooge/scrooge-core/src/main/scala",
        "util/util-app",
        "util/util-codec",
        "util/util-core/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
)

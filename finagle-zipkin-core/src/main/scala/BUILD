scala_library(
    sources = ["**/*.scala"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-zipkin-core",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-annotations",
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-core",
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind",
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-core/src/main",
        "finagle/finagle-thrift/src/main/java",
        "util/util-core:scala",
    ],
)

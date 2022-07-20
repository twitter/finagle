java_library(
    sources = ["**/*.java"],
    compiler_option_sets = [],
    platform = "java8",
    provides = artifact(
        org = "com.twitter",
        name = "finagle-example-java",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind",
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "finagle/finagle-base-http/src/main",
        "finagle/finagle-core/src/main",
        "finagle/finagle-example/src/main/thrift:thrift-scala",
        "finagle/finagle-http/src/main/scala",
        "finagle/finagle-thrift",
        "scrooge/scrooge-core/src/main/scala",
        "util/util-core",
    ],
)

jvm_binary(
    name = "thrift-server",
    main = "com.twitter.finagle.example.java.thrift.ThriftServer",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":java",
    ],
)

jvm_binary(
    name = "thrift-client",
    main = "com.twitter.finagle.example.java.thrift.ThriftClient",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":java",
    ],
)

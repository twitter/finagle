java_library(
    sources = rglobs("*.java"),
    fatal_warnings = False,
    provides = artifact(
        org = "com.twitter",
        name = "finagle-example-java",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind",
        "3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala",
        "3rdparty/jvm/io/netty",
        "finagle/finagle-base-http/src/main/scala",
        "finagle/finagle-core/src/main/scala",
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
    dependencies = [
        ":java",
    ],
)

jvm_binary(
    name = "thrift-client",
    main = "com.twitter.finagle.example.java.thrift.ThriftClient",
    dependencies = [
        ":java",
    ],
)

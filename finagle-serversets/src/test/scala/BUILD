target(
    dependencies = [
        ":client",
        ":defaultloader",
        ":serversets",
    ],
)

target(
    name = "core",
    dependencies = [
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalatest",
        "finagle/finagle-core",
        "finagle/finagle-serversets",
        "src/java/com/twitter/common/io",
        "src/java/com/twitter/common/quantity",
        "src/java/com/twitter/common/zookeeper:client",
        "src/java/com/twitter/common/zookeeper:server-set",
        "util/util-core",
        "util/util-zk-test",
    ],
)

junit_tests(
    name = "defaultloader",
    sources = globs("com/twitter/finagle/serverset2/client/apache/*.scala"),
    dependencies = [
        ":core",
        "3rdparty/jvm/org/apache/zookeeper:zookeeper-client",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-serversets/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
)

junit_tests(
    name = "client",
    sources = globs("com/twitter/finagle/serverset2/client/*.scala"),
    fatal_warnings = True,
    dependencies = [
        ":core",
        "finagle/finagle-serversets/src/main/scala",
        "util/util-core/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
)

junit_tests(
    name = "serversets",
    sources = globs(
        "com/twitter/*.scala",
        "com/twitter/finagle/serverset2/*.scala",
        "com/twitter/finagle/serverset2/addr/*.scala",
        "com/twitter/finagle/serverset2/naming/*.scala",
        "com/twitter/finagle/zookeeper/*.scala",
    ),
    dependencies = [
        ":core",
        "3rdparty/jvm/com/google/inject:guice",
        "finagle/finagle-core/src/main/scala",
        "finagle/finagle-serversets/src/main/java",
        "finagle/finagle-serversets/src/main/scala",
        "finagle/finagle-serversets/src/test/resources",
        "src/java/com/twitter/common/io:json",
        "src/java/com/twitter/common/net:dynamic-host-set",
        "src/thrift/com/twitter/thrift:thrift-java",
        "util/util-core/src/main/scala",
        "util/util-stats/src/main/scala",
        "util/util-zk-test/src/main/scala",
    ],
)

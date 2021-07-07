test_suite(
    tags = ["bazel-compatible"],
    dependencies = [
        ":client",
        ":defaultloader",
        ":serversets",
    ],
)

target(
    name = "core",
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/junit",
        "3rdparty/jvm/org/mockito:mockito-all",
        "3rdparty/jvm/org/scalatest",
        "3rdparty/jvm/org/scalatestplus:mockito-1-10",
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
    sources = ["com/twitter/finagle/serverset2/client/apache/*.scala"],
    tags = ["bazel-compatible"],
    dependencies = [
        ":core",
        "3rdparty/jvm/org/apache/zookeeper:zookeeper-client",
        "3rdparty/jvm/org/scalatestplus:junit",
        "finagle/finagle-core/src/main",
        "finagle/finagle-serversets/src/main/scala",
        "util/util-core:scala",
        "util/util-stats/src/main/scala",
    ],
)

junit_tests(
    name = "client",
    sources = ["com/twitter/finagle/serverset2/client/*.scala"],
    compiler_option_sets = ["fatal_warnings"],
    tags = ["bazel-compatible"],
    dependencies = [
        ":core",
        "3rdparty/jvm/org/scalatestplus:junit",
        "finagle/finagle-serversets/src/main/scala",
        "util/util-core:scala",
        "util/util-stats/src/main/scala",
    ],
)

junit_tests(
    name = "serversets",
    sources = [
        "com/twitter/*.scala",
        "com/twitter/finagle/serverset2/*.scala",
        "com/twitter/finagle/serverset2/addr/*.scala",
        "com/twitter/finagle/serverset2/naming/*.scala",
        "com/twitter/finagle/zookeeper/*.scala",
    ],
    tags = ["bazel-compatible"],
    dependencies = [
        ":core",
        "3rdparty/jvm/com/google/inject:guice",
        "3rdparty/jvm/org/scalacheck",
        "3rdparty/jvm/org/scalatestplus:junit",
        "finagle/finagle-core/src/main",
        "finagle/finagle-serversets/src/main/java",
        "finagle/finagle-serversets/src/main/scala",
        "finagle/finagle-serversets/src/test/resources",
        "src/java/com/twitter/common/io:json",
        "src/java/com/twitter/common/net:dynamic-host-set",
        "src/thrift/com/twitter/thrift:thrift-java",
        "util/util-core:scala",
        "util/util-stats/src/main/scala",
        "util/util-zk-test/src/main/scala",
    ],
)

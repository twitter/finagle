scala_library(
    sources = ["**/*.scala"],
    platform = "java8",
    tags = [
        "bazel-compatible",
        "bazel-incompatible-scaladoc",  # see http://go/bazel-incompatible-scaladoc
    ],
    dependencies = [
        "3rdparty/jvm/com/github/luben:zstd-jni",
        "3rdparty/jvm/io/netty:netty4-codec-http",
        "3rdparty/jvm/org/apache/curator:curator-framework",
        "3rdparty/jvm/org/apache/curator:curator-test",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "3rdparty/jvm/org/apache/zookeeper:zookeeper-client",
        "3rdparty/jvm/org/openjdk/jmh:jmh-core",
        "3rdparty/jvm/org/scala-lang:scala-library",
        "finagle/finagle-benchmark-thrift/src/main/thrift:thrift-scala",
        "finagle/finagle-core/src/main",
        "finagle/finagle-core/src/test/scala:stringprotocol",
        "finagle/finagle-exp/src/main/scala",
        "finagle/finagle-http/src/main/scala",
        "finagle/finagle-memcached/src/main/scala",
        "finagle/finagle-mux/src/main/scala",
        "finagle/finagle-mysql/src/main/scala",
        "finagle/finagle-netty4/src/main/scala",
        "finagle/finagle-postgresql/src/main/scala/com/twitter/finagle",
        "finagle/finagle-serversets/src/main/scala",
        "finagle/finagle-stats/src/main/scala",
        "finagle/finagle-thrift/src/main/java",
        "finagle/finagle-thrift/src/main/scala",
        "finagle/finagle-thriftmux/src/main/scala",
        "finagle/finagle-zipkin-scribe/src/main/scala",
        "scrooge/scrooge-core/src/main/scala",
        "util/util-app/src/main/scala",
        "util/util-core:scala",
        "util/util-logging/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
)

jvm_binary(
    name = "lbsim",
    main = "com.twitter.finagle.loadbalancer.Simulation",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
        "finagle/finagle-benchmark/src/main/resources",
    ],
)

jvm_binary(
    name = "hello-server",
    main = "com.twitter.finagle.thrift.HelloServer",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [":scala"],
)

jvm_binary(
    name = "serverset-service",
    main = "com.twitter.finagle.serverset2.LocalServerSetService",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [":scala"],
)

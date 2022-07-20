scala_library(
    sources = ["**/*.scala"],
    platform = "java8",
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-example",
        repo = artifactory,
    ),
    tags = ["bazel-compatible"],
    dependencies = [
        "3rdparty/jvm/com/google/inject:guice",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-base-http/src/main",
        "finagle/finagle-core/src/main",
        "finagle/finagle-example/src/main/thrift:thrift-scala",
        "finagle/finagle-http/src/main/scala",
        "finagle/finagle-memcached/src/main/scala",
        "finagle/finagle-mysql",
        "finagle/finagle-redis",
        "finagle/finagle-stats",
        "finagle/finagle-thrift",
        "scrooge/scrooge-core/src/main/scala",
        "util/util-app/src/main/scala",
        "util/util-codec/src/main/scala",
        "util/util-core:scala",
        "util/util-stats/src/main/scala",
    ],
)

jvm_binary(
    name = "http-client",
    main = "com.twitter.finagle.example.http.HttpClient",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "http-server",
    main = "com.twitter.finagle.example.http.HttpServer",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "http-streaming-client",
    main = "com.twitter.finagle.example.http.HttpStreamingClient",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "http-streaming-server",
    main = "com.twitter.finagle.example.http.HttpStreamingServer",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "kestrel-client",
    main = "com.twitter.finagle.example.kestrel.KestrelClient",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "memcached-ketama-stress",
    main = "com.twitter.finagle.example.memcache.KetamaClientStress",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "memcached-proxy",
    main = "com.twitter.finagle.example.memcachedproxy.MemcachedProxy",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "memcached-stress",
    main = "com.twitter.finagle.example.memcache.MemcacheStress",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "mysql",
    main = "com.twitter.finagle.example.mysql.Example",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "redis-client",
    main = "com.twitter.finagle.example.redis.RedisClient",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "stress",
    main = "com.twitter.finagle.example.stress.Stress",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "thrift-client",
    main = "com.twitter.finagle.example.thrift.ThriftClient",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "thrift-server",
    main = "com.twitter.finagle.example.thrift.ThriftServer",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

jvm_binary(
    name = "thrift-service-iface",
    main = "com.twitter.finagle.example.thrift.ThriftServicePerEndpointExample",
    platform = "java8",
    tags = ["bazel-compatible"],
    dependencies = [
        ":scala",
    ],
)

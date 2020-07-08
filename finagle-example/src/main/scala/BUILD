scala_library(
    sources = ["**/*.scala"],
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-example",
        repo = artifactory,
    ),
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
  name="http-client",
  main="com.twitter.finagle.example.http.HttpClient",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="http-server",
  main="com.twitter.finagle.example.http.HttpServer",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="http-streaming-client",
  main="com.twitter.finagle.example.http.HttpStreamingClient",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="http-streaming-server",
  main="com.twitter.finagle.example.http.HttpStreamingServer",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="kestrel-client",
  main="com.twitter.finagle.example.kestrel.KestrelClient",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="memcached-ketama-stress",
  main="com.twitter.finagle.example.memcache.KetamaClientStress",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="memcached-proxy",
  main="com.twitter.finagle.example.memcachedproxy.MemcachedProxy",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="memcached-stress",
  main="com.twitter.finagle.example.memcache.MemcacheStress",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="mysql",
  main="com.twitter.finagle.example.mysql.Example",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="redis-client",
  main="com.twitter.finagle.example.redis.RedisClient",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="stress",
  main="com.twitter.finagle.example.stress.Stress",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="thrift-client",
  main="com.twitter.finagle.example.thrift.ThriftClient",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="thrift-server",
  main="com.twitter.finagle.example.thrift.ThriftServer",
  dependencies=[
    ':scala'
  ]
)

jvm_binary(
  name="thrift-service-iface",
  main="com.twitter.finagle.example.thrift.ThriftServicePerEndpointExample",
  dependencies=[
    ':scala'
  ]
)

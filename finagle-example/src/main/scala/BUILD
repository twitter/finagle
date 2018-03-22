scala_library(
    sources = rglobs("*.scala"),
    provides = scala_artifact(
        org = "com.twitter",
        name = "finagle-example",
        repo = artifactory,
    ),
    dependencies = [
        "3rdparty/jvm/com/google/inject:guice",
        "3rdparty/jvm/org/apache/thrift:libthrift",
        "finagle/finagle-base-http/src/main/scala",
        "finagle/finagle-core/src/main/scala",
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
        "util/util-core/src/main/scala",
        "util/util-stats/src/main/scala",
    ],
)

def binary(name, main=None):
  return jvm_binary(name=name,
    main=main,
    dependencies=[
      ':scala'
    ]
  )

binary("http-client", "com.twitter.finagle.example.http.HttpClient")

binary("http-server", "com.twitter.finagle.example.http.HttpServer")

binary("http-streaming-client", "com.twitter.finagle.example.http.HttpStreamingClient")

binary("http-streaming-server", "com.twitter.finagle.example.http.HttpStreamingServer")

binary("kestrel-client", "com.twitter.finagle.example.kestrel.KestrelClient")

binary("memcached-ketama-stress", "com.twitter.finagle.example.memcache.KetamaClientStress")

binary("memcached-proxy", "com.twitter.finagle.example.memcachedproxy.MemcachedProxy")

binary("memcached-stress", "com.twitter.finagle.example.memcache.MemcacheStress")

binary("mysql", "com.twitter.finagle.example.mysql.Example")

binary("redis-client", "com.twitter.finagle.example.redis.RedisClient")

binary("stress", "com.twitter.finagle.example.stress.Stress")

binary("thrift-client", "com.twitter.finagle.example.thrift.ThriftClient")

binary("thrift-server", "com.twitter.finagle.example.thrift.ThriftServer")

binary("thrift-service-iface", "com.twitter.finagle.example.thrift.ThriftServiceIfaceExample")

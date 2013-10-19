scala_library(name='scala',
  dependencies=[
    jar(org='com.twitter.common', name='flags', rev='1.0.0')
      .exclude(org='com.twitter'),
    pants('finagle/finagle-core'),
    pants('finagle/finagle-example/src/main/thrift:compiled'),
    pants('finagle/finagle-http'),
    pants('finagle/finagle-kestrel'),
    pants('finagle/finagle-memcached'),
    pants('finagle/finagle-mysql'),
    pants('finagle/finagle-ostrich4'),
    pants('finagle/finagle-redis'),
    pants('finagle/finagle-stats'),
    pants('finagle/finagle-stream'),
    pants('finagle/finagle-thrift'),
    pants('util/util-codec'),
  ],
  sources=rglobs('*.scala'),
)

def binary(name, main=None):
  return jvm_binary(name=name,
    main=main,
    dependencies=[
      pants(':scala')
    ]
  )

binary('http-client', 'com.twitter.finagle.example.http.HttpClient')
binary('http-server', 'com.twitter.finagle.example.http.HttpServer')
binary('kestrel-client', 'com.twitter.finagle.example.kestrel.KestrelClient')
binary('memcached-proxy', 'com.twitter.finagle.example.memcachedproxy.MemcachedProxy')
binary('memcached-ketama-stress', 'com.twitter.finagle.example.memcache.KetamaClientStress')
binary('memcached--stress', 'com.twitter.finagle.example.memcache.MemcacheStress')
binary('mysql', 'com.twitter.finagle.example.mysql.Example')
binary('redis-client', 'com.twitter.finagle.example.redis.RedisClient')
binary('hosebird-client', 'com.twitter.finagle.example.stream.HosebirdClient')
binary('stream-server', 'com.twitter.finagle.example.stream.StreamServer')
binary('stress', 'com.twitter.finagle.example.stress.Stress')
binary('thrift-client', 'com.twitter.finagle.example.thrift.ThriftClient')
binary('thrift-server', 'com.twitter.finagle.example.thrift.ThriftServer')

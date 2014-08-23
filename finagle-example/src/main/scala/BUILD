scala_library(name='scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-example',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    'finagle/finagle-core',
    'finagle/finagle-example/src/main/thrift:thrift-scala',
    'finagle/finagle-http',
    'finagle/finagle-kestrel',
    'finagle/finagle-memcached',
    'finagle/finagle-mysql',
    'finagle/finagle-ostrich4',
    'finagle/finagle-redis',
    'finagle/finagle-stats',
    'finagle/finagle-stream',
    'finagle/finagle-thrift',
    'util/util-codec',
  ],
  sources=rglobs('*.scala'),
)

def binary(name, main=None):
  return jvm_binary(name=name,
    main=main,
    dependencies=[
      ':scala'
    ]
  )

binary('http-client', 'com.twitter.finagle.example.http.HttpClient')
binary('http-server', 'com.twitter.finagle.example.http.HttpServer')
binary('kestrel-client', 'com.twitter.finagle.example.kestrel.KestrelClient')
binary('memcached-proxy', 'com.twitter.finagle.example.memcachedproxy.MemcachedProxy')
binary('memcached-ketama-stress', 'com.twitter.finagle.example.memcache.KetamaClientStress')
binary('memcached-stress', 'com.twitter.finagle.example.memcache.MemcacheStress')
binary('mysql', 'com.twitter.finagle.example.mysql.Example')
binary('redis-client', 'com.twitter.finagle.example.redis.RedisClient')
binary('hosebird-client', 'com.twitter.finagle.example.stream.HosebirdClient')
binary('stream-server', 'com.twitter.finagle.example.stream.StreamServer')
binary('stress', 'com.twitter.finagle.example.stress.Stress')
binary('thrift-client', 'com.twitter.finagle.example.thrift.ThriftClient')
binary('thrift-server', 'com.twitter.finagle.example.thrift.ThriftServer')

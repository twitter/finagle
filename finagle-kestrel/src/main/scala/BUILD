scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-kestrel',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/github/ben-manes/caffeine',
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'finagle/finagle-memcached',
    'finagle/finagle-thrift',
    'finagle/finagle-thrift/src/main/java',
    'finagle/finagle-kestrel/src/main/thrift:thrift-scala',
    'util/util-core',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

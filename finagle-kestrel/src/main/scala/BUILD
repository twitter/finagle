scala_library(name='scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-kestrel',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty:guava',
    '3rdparty:netty',
    'finagle/finagle-core',
    'finagle/finagle-memcached',
    'finagle/finagle-thrift',
    'finagle/finagle-thrift/src/main/java',
    'finagle/finagle-kestrel/src/main/thrift:thrift-scala',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-kestrelx',
    repo = 'pants-support/ivy:internal',
  ),
  dependencies=[
    '3rdparty:guava',
    '3rdparty:netty',
    'finagle/finagle-core',
    'finagle/finagle-memcachedx',
    'finagle/finagle-thrift',
    'finagle/finagle-thrift/src/main/java',
    'finagle/finagle-kestrelx/src/main/thrift:thrift-scala',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
)

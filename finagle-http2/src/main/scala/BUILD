scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-http2',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty:netty',
    '3rdparty/jvm/io/netty:netty4',
    '3rdparty/jvm/io/netty:netty4-http2',
    'finagle/finagle-core',
    'finagle/finagle-netty4',
    'util/util-core',
  ],
  strict_deps=True,
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

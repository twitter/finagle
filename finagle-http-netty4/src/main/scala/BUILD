scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-http-netty4',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty:netty4',
    '3rdparty/jvm/io/netty:netty4-http',
    'finagle/finagle-core',
    'finagle/finagle-http',
    'finagle/finagle-netty4',
    'util/util-core',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

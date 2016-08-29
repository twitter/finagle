scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-netty4-http',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty:netty4',
    '3rdparty/jvm/io/netty:netty4-http',
    'finagle/finagle-netty4',
    '3rdparty/jvm/io/netty:netty4-handler-proxy',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-http/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

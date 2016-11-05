scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-netty4-http',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty:netty4',
    '3rdparty/jvm/io/netty:netty4-http',
    '3rdparty/jvm/io/netty:netty4-handler-proxy',
    'finagle/finagle-netty4',
    'finagle/finagle-base-http',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

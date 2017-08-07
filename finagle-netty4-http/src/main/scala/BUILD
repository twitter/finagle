scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-netty4-http',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty:netty',
    '3rdparty/jvm/io/netty:netty4',
    '3rdparty/jvm/io/netty:netty4-http',
    '3rdparty/jvm/io/netty:netty4-handler-proxy',
    'finagle/finagle-base-http',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-netty4',
    'util/util-core/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
  strict_deps=True,
)

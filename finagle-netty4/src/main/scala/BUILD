scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-netty4',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty:netty4',
    '3rdparty/jvm/io/netty:netty4-codec',
    '3rdparty/jvm/io/netty:netty4-http',
    '3rdparty/jvm/io/netty:netty4-handler-proxy',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-netty4/src/main/resources',
    'finagle/finagle-toggle',
    'util/util-app/src/main/scala:scala',
    'util/util-codec/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-jvm/src/main/scala:scala',
    'util/util-lint/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-netty4-http',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty:netty',
    '3rdparty/jvm/io/netty:netty4',
    '3rdparty/jvm/io/netty:netty4-handler-proxy',
    '3rdparty/jvm/io/netty:netty4-http',
    'finagle/finagle-base-http',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-netty4',
    'util/util-core/src/main/scala:scala',
    'util/util-logging/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
  exports=[
    '3rdparty/jvm/io/netty:netty4',
  ]

)

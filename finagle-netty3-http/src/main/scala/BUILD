scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-netty3-http',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty:netty',
    'finagle/finagle-base-http',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-netty3',
    'util/util-core/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

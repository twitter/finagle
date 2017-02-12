scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-redis',
    repo = artifactory,
  ),
  dependencies=[
    'util/util-logging',
    '3rdparty/jvm/io/netty:netty',
    'finagle/finagle-core',
    'finagle/finagle-netty4',
    'util/util-core',
    'util/util-stats',
  ],
  sources=rglobs('*.scala'),
)

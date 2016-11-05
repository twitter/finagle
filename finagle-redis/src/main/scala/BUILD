scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-redis',
    repo = artifactory,
  ),
  dependencies=[
    'util/util-logging',
    '3rdparty/jvm/io/netty:netty',
    'finagle/finagle-core/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
)

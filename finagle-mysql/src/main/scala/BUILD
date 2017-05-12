scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-mysql',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/github/ben-manes/caffeine',
    '3rdparty/jvm/com/google/code/findbugs:jsr305',
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-mysql/src/main/resources',
    'finagle/finagle-netty4',
    'finagle/finagle-toggle',
    'util/util-cache',
    'util/util-core/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
)

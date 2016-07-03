scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-mysql',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/github/ben-manes/caffeine',
    '3rdparty/jvm/com/google/code/findbugs:jsr305',
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'finagle/finagle-netty4',
    'util/util-core',
    'util/util-cache',
    'util/util-logging'
  ],
  sources=rglobs('*.scala'),
)

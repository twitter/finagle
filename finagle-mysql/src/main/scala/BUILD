scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-mysql',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'util/util-core',
    'util/util-cache',
    'util/util-logging'
  ],
  sources=rglobs('*.scala'),
)

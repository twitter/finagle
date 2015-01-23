scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-testers',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
)

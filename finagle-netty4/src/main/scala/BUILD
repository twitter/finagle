scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-netty4',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty:netty4',
    'finagle/finagle-core',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-ostrich4',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'finagle/finagle-http',
    'ostrich',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-ostrich4/src/main/resources'
  ],
)

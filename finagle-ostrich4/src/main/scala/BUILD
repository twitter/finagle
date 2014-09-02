scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-ostrich4',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty:netty',
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

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-testers',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty:netty',
    'finagle/finagle-core',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
)

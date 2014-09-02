scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-mysql',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty:netty',
    'finagle/finagle-core',
    'util/util-core',
    'util/util-logging'
  ],
  sources=rglobs('*.scala'),
)

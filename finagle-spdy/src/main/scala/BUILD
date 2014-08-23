scala_library(name='scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-spdy',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty:netty',
    'finagle/finagle-core',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
)

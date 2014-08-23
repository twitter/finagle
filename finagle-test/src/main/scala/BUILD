scala_library(name='scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-test',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty:netty',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
)

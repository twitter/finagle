scala_library(name='scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-commons-stats',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty/jvm/com/twitter/common:stats',
    'finagle/finagle-core'
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-commons-stats/src/main/resources'
  ],
)

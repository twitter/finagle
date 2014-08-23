scala_library(name='scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-redis',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    'finagle/finagle-core',
    'util/util-core',
    'util/util-logging'
  ],
  sources=rglobs('*.scala'),
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-redis',
    repo = 'pants-support/ivy:internal',
  ),
  dependencies=[
    'finagle/finagle-core',
    'util/util-core',
    'util/util-logging'
  ],
  sources=rglobs('*.scala'),
)

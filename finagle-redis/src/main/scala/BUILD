scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-redis',
    repo = artifactory,
  ),
  dependencies=[
    'finagle/finagle-core',
    'util/util-core',
    'util/util-logging'
  ],
  sources=rglobs('*.scala'),
)

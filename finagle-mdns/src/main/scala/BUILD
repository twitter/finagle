scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-mdns',
    repo = 'pants-support/ivy:internal',
  ),
  dependencies=[
    '3rdparty:jmdns',
    'finagle/finagle-core',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-mdns/src/main/resources'
  ],
)

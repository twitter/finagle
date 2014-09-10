scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-stream',
    repo = 'pants-support/ivy:internal',
  ),
  dependencies=[
    '3rdparty:netty',
    'finagle/finagle-core',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
)

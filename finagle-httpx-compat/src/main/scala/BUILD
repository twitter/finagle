scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-httpx-compat',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'finagle/finagle-httpx',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
)

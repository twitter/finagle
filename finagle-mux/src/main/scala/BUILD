scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-mux',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    '3rdparty/jvm/com/twitter/common:quantity',
    '3rdparty/jvm/com/twitter/common:stats-util',
    '3rdparty/jvm/com/twitter/common:util-system-mocks',
    'finagle/finagle-core',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
)

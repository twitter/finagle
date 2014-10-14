scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-mux',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty:netty',
    'finagle/finagle-core',
    '3rdparty/jvm/com/twitter/common:stats-util',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-mux',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'finagle/finagle-netty4',
    'science/src/java/com/twitter/common/quantity:quantity',
    'science/src/java/com/twitter/common/stats:util',
    'science/src/java/com/twitter/common/util:system-mocks',
    'util/util-core',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

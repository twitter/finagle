scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-exp',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/twitter:jsr166e',
    'finagle/finagle-core',
    'finagle/finagle-thrift',
    'util/util-core',
  ],
  sources=rglobs('com/twitter/finagle/exp/*.scala'),
)

benchmark(name='benchmark',
  dependencies=[
    ':scala',
    '3rdparty/jvm/com/google/caliper',
    'util/util-core',
  ],
  sources=rglobs('com/twitter/finagle/benchmark/*.scala')
)

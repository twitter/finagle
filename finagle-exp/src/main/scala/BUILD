scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-exp',
    repo = artifactory,
  ),
  dependencies=[
    'finagle/finagle-core',
    'finagle/finagle-thrift',
    'util/util-core',
  ],
  sources=rglobs('com/twitter/finagle/exp/*.scala'),
)

benchmark(name='benchmark',
  dependencies=[
    ':scala',
    'util/util-core',
  ],
  sources=rglobs('com/twitter/finagle/benchmark/*.scala')
)

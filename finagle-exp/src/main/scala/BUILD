scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-exp',
    repo = 'pants-support/ivy:internal',
  ),
  dependencies=[
    '3rdparty:jsr166e',
    'finagle/finagle-core',
    'finagle/finagle-thrift',
    'util/util-core',
  ],
  sources=rglobs('com/twitter/finagle/exp/*.scala'),
)

benchmark(name='benchmark',
  dependencies=[
    ':scala',
    '3rdparty:caliper',
    'util/util-core',
  ],
  sources=rglobs('com/twitter/finagle/benchmark/*.scala')
)

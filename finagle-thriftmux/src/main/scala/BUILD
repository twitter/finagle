scala_library(name='scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-thriftmux',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-mux',
    'finagle/finagle-thrift',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-thriftmux',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/org/apache/thrift:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-mux',
    'finagle/finagle-thrift',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-thrift',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty:netty',
    '3rdparty:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-thrift/src/main/java',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-thrift/src/main/resources'
  ],
)

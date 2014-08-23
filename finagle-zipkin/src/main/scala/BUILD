scala_library(name='scala',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-zipkin',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-thrift',
    'finagle/finagle-zipkin/src/main/thrift:thrift-scala',
    'util/util-app',
    'util/util-codec',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-zipkin/src/main/resources'
  ],
)

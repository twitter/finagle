scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-zipkin',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/org/apache/thrift:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-thrift',
    'finagle/finagle-zipkin/src/main/thrift:thrift-scala',
    'util/util-app',
    'util/util-codec',
    'util/util-core',
    'util/util-events',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-zipkin/src/main/resources'
  ],
)

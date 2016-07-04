scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-zipkin-kafka',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/org/apache/kafka:kafka-clients-0.8.2.2',
    '3rdparty/jvm/org/apache/thrift:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-thrift',
    'finagle/finagle-zipkin-core',
    'util/util-app',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-zipkin-kafka/src/main/resources'
  ],
)

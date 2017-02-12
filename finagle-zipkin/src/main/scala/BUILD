scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-zipkin',
    repo = artifactory,
  ),
  dependencies=[
    'finagle/finagle-thrift',
    'finagle/finagle-zipkin-core',
    'finagle/finagle-zipkin/src/main/thrift:thrift-scala',
    'util/util-app',
    '3rdparty/jvm/com/google/guava:guava',
    '3rdparty/jvm/org/apache/thrift:libthrift-0.5.0',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-thrift/src/main/java:java',
    'scrooge/scrooge-core/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-zipkin/src/main/resources'
  ],
  # Exposes a service-loaded class.
  scope='forced',
)

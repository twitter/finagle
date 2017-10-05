scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-zipkin',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/google/guava:guava',
    '3rdparty/jvm/org/apache/thrift:libthrift-0.5.0',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-thrift',
    'finagle/finagle-thrift/src/main/java:java',
    'finagle/finagle-zipkin-core',
    'finagle/finagle-zipkin/src/main/resources',
    'finagle/finagle-zipkin/src/main/thrift:thrift-scala',
    'scrooge/scrooge-core/src/main/scala:scala',
    'util/util-app',
    'util/util-core/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
  # Exposes a service-loaded class.
  scope='forced',
)

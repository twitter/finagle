scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-thriftmux',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/org/apache/thrift:libthrift-0.5.0',
    '3rdparty/jvm/io/netty:netty4',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-mux/src/main/scala:scala',
    'finagle/finagle-thrift',
    'finagle/finagle-thrift/src/main/java:java',
    'finagle/finagle-thriftmux/src/main/resources',
    'finagle/finagle-toggle/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-logging/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  exports=[
    'finagle/finagle-mux/src/main/scala:scala',
    'finagle/finagle-thrift',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

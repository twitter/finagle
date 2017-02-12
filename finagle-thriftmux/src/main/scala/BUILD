scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-thriftmux',
    repo = artifactory,
  ),
  dependencies=[
    'finagle/finagle-thrift',
    '3rdparty/jvm/org/apache/thrift:libthrift-0.5.0',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-mux/src/main/scala:scala',
    'finagle/finagle-thrift/src/main/java:java',
    'util/util-core/src/main/scala:scala',
    'util/util-logging/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-exception',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala',
    '3rdparty/jvm/org/apache/thrift:libthrift-0.5.0',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-exception/src/main/thrift:thrift-scala',
    'finagle/finagle-thrift',
    'finagle/finagle-thrift/src/main/java:java',
    'scrooge/scrooge-core/src/main/scala:scala',
    'util/util-app',
    'util/util-codec',
    'util/util-core/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
  fatal_warnings=True,
  strict_deps=True,
)

scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-zipkin-core',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-core',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-annotations',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-thrift/src/main/java:java',
    'util/util-core/src/main/scala:scala',
    'util/util-events/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-exception',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala',
    'finagle/finagle-thrift',
    'util/util-app',
    'util/util-codec',
    'finagle/finagle-exception/src/main/thrift:thrift-scala',
    'finagle/finagle-core/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
)

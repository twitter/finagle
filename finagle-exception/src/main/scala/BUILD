scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-exception',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala',
    '3rdparty/jvm/org/apache/thrift:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-thrift',
    'util/util-app',
    'util/util-codec',
    'util/util-core',
    'finagle/finagle-exception/src/main/thrift:thrift-scala',
  ],
  sources=rglobs('*.scala'),
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-stats',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-core',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala',
    '3rdparty/jvm/com/twitter/common:metrics',
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'finagle/finagle-http',
    'util/util-events',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-stats/src/main/resources'
  ],
)

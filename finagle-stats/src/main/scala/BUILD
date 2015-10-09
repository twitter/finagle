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
    'science/src/java/com/twitter/common/metrics:metrics',
    'science/src/java/com/twitter/common/quantity:quantity',
    'science/src/java/com/twitter/common/stats:util',
    'science/src/java/com/twitter/common/util:util',
    'science/src/java/com/twitter/common/util:system-mocks',
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'finagle/finagle-httpx',
    'util/util-core',
    'util/util-events',
    'util/util-logging',
    'util/util-registry',
    'util/util-stats',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-stats/src/main/resources'
  ],
)

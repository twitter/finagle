scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-commons-stats',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-core',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala',
    'finagle/finagle-http',
    'src/java/com/twitter/common/base:base',
    'src/java/com/twitter/common/stats:stats',
    'src/java/com/twitter/common/stats:stats-provider',
    'util/util-core',
    'util/util-registry',
    'util/util-stats',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-commons-stats/src/main/resources'
  ],
  # Exposes a service-loaded class.
  scope='forced',
)

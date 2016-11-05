scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-commons-stats',
    repo = artifactory,
  ),
  dependencies=[
    'science/src/java/com/twitter/common/stats:stats',
    'util/util-registry',
    'util/util-stats',
    'science/src/java/com/twitter/common/base:base',
    'science/src/java/com/twitter/common/stats:stats-provider',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-commons-stats/src/main/resources'
  ],
  # Exposes a service-loaded class.
  scope='forced',
)

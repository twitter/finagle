scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-commons-stats',
    repo = artifactory,
  ),
  dependencies=[
    'science/src/java/com/twitter/common/stats:stats',
    'finagle/finagle-core',
    'util/util-registry',
    'util/util-stats',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-commons-stats/src/main/resources'
  ],
)

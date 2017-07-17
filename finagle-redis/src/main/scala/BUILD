scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-redis',
    repo = artifactory,
  ),
  dependencies=[
    'util/util-logging',
    'finagle/finagle-core',
    'finagle/finagle-netty4',
    'util/util-core',
    'util/util-stats',
  ],
  sources=rglobs('*.scala'),
  strict_deps=True,
)

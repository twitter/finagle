scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-redis',
    repo = artifactory,
  ),
  dependencies=[
    'finagle/finagle-core',
    'finagle/finagle-netty4',
    'util/util-core',
    'util/util-logging',
    'util/util-stats',
  ],
  sources=rglobs('*.scala'),
  exports=[
    'finagle/finagle-core:finagle-core',
    'util/util-core:util-core',
  ]

)

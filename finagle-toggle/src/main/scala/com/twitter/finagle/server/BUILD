scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-server',
    repo = artifactory,
  ),
  dependencies=[
    'util/util-core/src/main/scala:scala',
    'util/util-registry/src/main/scala:scala',
  ],
  fatal_warnings=True,
  strict_deps=True,
  sources=rglobs('*.scala'),
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-toggle',
    repo = artifactory,
  ),
  dependencies=[
    'util/util-app/src/main/java',
    'util/util-app/src/main/scala',
    'util/util-core',
  ],
  fatal_warnings=True,
  strict_deps=True,
  sources=rglobs('*.scala'),
)

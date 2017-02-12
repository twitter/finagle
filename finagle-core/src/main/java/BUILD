java_library(
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-core-java',
    repo = artifactory,
  ),
  sources=rglobs('*.java'),
  dependencies=[
    'finagle/finagle-core/src/main/scala',
    'util/util-core/src/main/java',
    'util/util-core/src/main/scala'
  ],
  fatal_warnings=True,
  strict_deps=True,
)

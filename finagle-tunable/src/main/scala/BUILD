scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-tunable',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala',
    'finagle/finagle-toggle/src/main/scala/com/twitter/finagle/server:server',
    'util/util-core/src/main/scala:scala',
    'util/util-tunable/src/main/scala:scala',
  ],
  fatal_warnings=True,
  strict_deps=True,
  sources=rglobs('*.scala'),
)

scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-netty3',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core/src/main/scala:scala',
    'util/util-app/src/main/scala:scala',
    'util/util-codec/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-jvm/src/main/scala:scala',
    'util/util-lint/src/main/scala:scala',
    'util/util-security/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  strict_deps=True,
  exports=[
    '3rdparty/jvm/io/netty',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

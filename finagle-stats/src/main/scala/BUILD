scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-stats',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-core',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala',
    'src/java/com/twitter/common/metrics:metrics',
    'src/java/com/twitter/common/stats:util',
    'util/util-logging',
    'util/util-registry',
    'util/util-stats',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-annotations',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-http/src/main/scala:scala',
    'finagle/finagle-stats/src/main/resources',
    'util/util-app/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-events/src/main/scala:scala',
    'util/util-lint/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
  # In the vast majority of cases, the consumers of finagle-stats only need it at runtime,
  # and so ideally we would mark this "scope=runtime". But there are enough consumers that also
  # need it at compiletime that it is non-trivial to split the target at this juncture.
  scope='forced',
)

scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-kestrel',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/github/ben-manes/caffeine',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-kestrel/src/main/resources',
    'finagle/finagle-kestrel/src/main/thrift:thrift-scala',
    'finagle/finagle-memcached/src/main/scala:scala',
    'finagle/finagle-netty4',
    'finagle/finagle-thrift',
    'finagle/finagle-thrift/src/main/java',
    'finagle/finagle-thriftmux',
    'finagle/finagle-toggle',
    'scrooge/scrooge-core/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
  # NB: This is not an actual dependency, but is necessary to have Pants behave
  #     like SBT, throwing an error for "import java.*" statements.
  java_sources=['finagle/finagle-kestrel/src/main/java'],
)

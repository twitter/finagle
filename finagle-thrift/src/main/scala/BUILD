scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-thrift',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/google/guava:guava',
    '3rdparty/jvm/io/netty',
    '3rdparty/jvm/org/apache/thrift:libthrift-0.5.0',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-netty4',
    'finagle/finagle-thrift/src/main/java',
    'finagle/finagle-thrift/src/main/resources',
    'finagle/finagle-toggle/src/main/scala:scala',
    'scrooge/scrooge-core',
    'util/util-app/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-logging/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
    '3rdparty/jvm/com/fasterxml/jackson:jackson-annotations',
  ],
  sources=rglobs('*.scala'),
  fatal_warnings=True,
)

# For usecases (like scrooge codegen) where this dependency is used, but not detected as
# used by zinc.
target(
  name='forced',
  scope='forced',
  dependencies=[':scala'],
)

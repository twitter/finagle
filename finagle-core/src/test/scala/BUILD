# the stringprotocol target doesn't have an sbt analog so
# don't depend on it in projects which need to support sbt.
scala_library(
  name='stringprotocol',
  dependencies=[
    '3rdparty/jvm/io/netty:netty',
    'finagle/finagle-core/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
  ],
  sources=[
    './com/twitter/finagle/client/StringClient.scala',
    './com/twitter/finagle/server/StringServer.scala'
  ],
  fatal_warnings=True,
)

junit_tests(
  dependencies=[
    '3rdparty/jvm/junit',
    '3rdparty/jvm/org/mockito:mockito-all',
    '3rdparty/jvm/org/scalacheck',
    '3rdparty/jvm/org/scalatest',
    'util/util-stats',
    '3rdparty/jvm/com/github/ben-manes/caffeine:caffeine',
    '3rdparty/jvm/io/netty:netty',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-core/src/test/resources',
    'util/util-app/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-hashing/src/main/scala:scala',
    'util/util-jvm/src/main/scala:scala',
    'util/util-logging/src/main/scala:scala',
    'util/util-registry/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
  fatal_warnings=True,
)

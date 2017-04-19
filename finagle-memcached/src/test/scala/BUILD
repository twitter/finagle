EXTERNAL_SOURCES = [
  'com/twitter/finagle/memcached/integration/ExternalMemcached.scala',
  'com/twitter/finagle/memcached/integration/InProcessMemcached.scala'
]

scala_library(name='external',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-memcached-testutils',
    repo = artifactory,
  ),
  dependencies=[
    'util/util-collection',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-memcached/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
  ],
  sources=EXTERNAL_SOURCES
)

junit_tests(
  dependencies=[
    ':external',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala',
    '3rdparty/jvm/io/netty',
    '3rdparty/jvm/junit',
    '3rdparty/jvm/org/apache/zookeeper:zookeeper-server',
    '3rdparty/jvm/org/mockito:mockito-all',
    '3rdparty/jvm/org/scalacheck',
    '3rdparty/jvm/org/scalatest',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-core/src/test/scala:scala',
    'finagle/finagle-memcached/src/main/scala:scala',
    'finagle/finagle-memcached/src/test/resources',
    'finagle/finagle-serversets',
    'util/util-collection/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-hashing',
    'util/util-registry/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
    'util/util-zk-test',
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala', exclude=[EXTERNAL_SOURCES]),
)

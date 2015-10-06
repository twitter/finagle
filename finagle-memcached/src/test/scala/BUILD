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
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'finagle/finagle-memcached',
    'util/util-collection',
    'util/util-core'
  ],
  sources=EXTERNAL_SOURCES
)

junit_tests(name='scala',
  dependencies=[
    ':external',
    '3rdparty/jvm/junit',
    '3rdparty/jvm/org/mockito:mockito-all',
    '3rdparty/jvm/io/netty',
    '3rdparty/jvm/org/scalacheck',
    '3rdparty/jvm/org/scalatest',
    '3rdparty/jvm/org/scala-tools/testing:specs',
    'science/src/java/com/twitter/common/application:action',
    'science/src/thrift/com/twitter/thrift:thrift-java',
    'science/src/java/com/twitter/common/zookeeper:testing',
    'science/src/java/com/twitter/common/zookeeper:client',
    'science/src/java/com/twitter/common/zookeeper:server-set',
    'finagle/finagle-core',
    'finagle/finagle-memcached',
    'finagle/finagle-serversets',
    'util/util-core',
    'util/util-hashing',
    'util/util-zk-test'
  ],
  sources=rglobs('*.scala', exclude=[EXTERNAL_SOURCES]),
  resources=[
    'finagle/finagle-memcached/src/test/resources'
  ]
)

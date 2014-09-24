EXTERNAL_SOURCES = [
  'com/twitter/finagle/memcachedx/integration/ExternalMemcached.scala',
  'com/twitter/finagle/memcachedx/integration/InProcessMemcached.scala'
]

scala_library(name='external',
  dependencies=[
    '3rdparty:netty',
    'finagle/finagle-core',
    'finagle/finagle-memcachedx',
    'util/util-collection',
    'util/util-core'
  ],
  sources=EXTERNAL_SOURCES
)

junit_tests(name='scala',
  dependencies=[
    ':external',
    '3rdparty:junit',
    '3rdparty:mockito-all',
    '3rdparty:netty',
    '3rdparty:scalatest',
    '3rdparty:specs',
    '3rdparty/jvm/com/twitter/common:application-action',
    '3rdparty/jvm/com/twitter/common:service-thrift',
    '3rdparty/jvm/com/twitter/common:zookeeper-testing',
    '3rdparty/jvm/com/twitter/common/zookeeper:client',
    '3rdparty/jvm/com/twitter/common/zookeeper:server-set',
    'finagle/finagle-core',
    'finagle/finagle-memcachedx',
    'finagle/finagle-serversets',
    'util/util-core',
    'util/util-hashing'
  ],
  sources=rglobs('*.scala') - EXTERNAL_SOURCES,
  resources=[
    'finagle/finagle-memcachedx/src/test/resources'
  ]
)

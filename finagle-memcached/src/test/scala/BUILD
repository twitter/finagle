EXTERNAL_SOURCES = [
  'com/twitter/finagle/memcached/integration/ExternalMemcached.scala',
  'com/twitter/finagle/memcached/integration/InProcessMemcached.scala'
]

scala_library(name='external',
  dependencies=[
    pants('3rdparty:netty'),
    pants('finagle/finagle-core'),
    pants('finagle/finagle-memcached'),
    pants('util/util-collection'),
    pants('util/util-core')
  ],
  sources=EXTERNAL_SOURCES
)

junit_tests(name='scala',
  dependencies=[
    pants(':external'),
    pants('3rdparty:junit'),
    pants('3rdparty:mockito-all'),
    pants('3rdparty:netty'),
    pants('3rdparty:scalatest'),
    pants('3rdparty:specs'),
    pants('3rdparty/com/twitter/common:application-action'),
    pants('3rdparty/com/twitter/common:service-thrift'),
    pants('3rdparty/com/twitter/common:zookeeper-testing'),
    pants('3rdparty/com/twitter/common/zookeeper:client'),
    pants('3rdparty/com/twitter/common/zookeeper:server-set'),
    pants('finagle/finagle-core'),
    pants('finagle/finagle-memcached'),
    pants('finagle/finagle-serversets'),
    pants('util/util-core'),
    pants('util/util-hashing')
  ],
  sources=rglobs('*.scala') - EXTERNAL_SOURCES,
  resources=[
    pants('finagle/finagle-memcached/src/test/resources')
  ]
)

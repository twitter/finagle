scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-memcached',
    repo = pants('pants-support/ivy:internal'),
  ),
  dependencies=[
    '3rdparty:gson',
    '3rdparty:guava',
    '3rdparty:jackson-core',
    '3rdparty:jackson-databind',
    '3rdparty:jackson-module-scala',
    '3rdparty:netty',
    '3rdparty:zookeeper',
    '3rdparty/jvm/com/twitter/common:io',
    '3rdparty/jvm/com/twitter/common:quantity',
    '3rdparty/jvm/com/twitter/common/zookeeper:client',
    '3rdparty/jvm/com/twitter/common/zookeeper:server-set',
    'finagle/finagle-core',
    'finagle/finagle-serversets',
    'util/util-core',
    'util/util-hashing',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-memcached/src/main/resources'
  ],
)

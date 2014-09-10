scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-serversets',
    repo = 'pants-support/ivy:internal',
  ),
  dependencies=[
    '3rdparty/jvm/com/twitter/common:service-thrift',
    '3rdparty/jvm/com/twitter/common/zookeeper:client',
    '3rdparty/jvm/com/twitter/common/zookeeper:server-set',
    '3rdparty:jackson-core',
    '3rdparty:jackson-databind',
    '3rdparty:slf4j-api',
    '3rdparty:guava',
    'finagle/finagle-core',
    'util/util-core',
    'util/util-zk-common'
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-serversets/src/main/resources'
  ],
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-serversets',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-core',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/google/guava',
    '3rdparty/jvm/com/twitter/common/zookeeper:client',
    '3rdparty/jvm/com/twitter/common/zookeeper:server-set',
    '3rdparty/jvm/com/twitter/common:service-thrift',
    '3rdparty:slf4j-api',
    'finagle/finagle-core',
    'util/util-core',
    'util/util-zk-common'
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-serversets/src/main/resources'
  ],
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-serversets',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-core',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    'science/src/java/com/twitter/common/zookeeper:client',
    'science/src/java/com/twitter/common/zookeeper:server-set',
    'science/src/thrift/com/twitter/thrift:thrift-java',
    '3rdparty/jvm/org/slf4j:slf4j-api',
    'finagle/finagle-core',
    'util/util-cache',
    'util/util-core',
    'util/util-zk-common'
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-serversets/src/main/resources'
  ],
)

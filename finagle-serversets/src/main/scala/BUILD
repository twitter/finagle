scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-serversets',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-core',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/github/ben-manes/caffeine',
    '3rdparty/jvm/org/slf4j:slf4j-api',
    'finagle/finagle-core',
    'science/src/java/com/twitter/common/zookeeper:client',
    'science/src/java/com/twitter/common/zookeeper:server-set',
    'science/src/thrift/com/twitter/thrift:thrift-java',
    'util/util-cache',
    'util/util-core',
    'util/util-zk-common'
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-serversets/src/main/resources'
  ],
)

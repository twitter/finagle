scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-serversets',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/github/ben-manes/caffeine',
    'src/java/com/twitter/common/zookeeper:client',
    'src/java/com/twitter/common/zookeeper:server-set',
    'src/thrift/com/twitter/thrift:thrift-java',
    'util/util-cache',
    'util/util-zk-common',
    '3rdparty/jvm/com/google/guava:guava',
    '3rdparty/jvm/com/google/inject:guice',
    '3rdparty/jvm/io/netty:netty',
    '3rdparty/jvm/org/apache/zookeeper:zookeeper-client',
    'finagle/finagle-core/src/main/scala:scala',
    'src/java/com/twitter/common/net:dynamic-host-set',
    'src/java/com/twitter/common/quantity:quantity',
    'util/util-app/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-logging/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-serversets/src/main/resources'
  ],
  # Exposes a service-loaded class.
  scope='forced',
)

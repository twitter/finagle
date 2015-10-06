scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-cacheresolver',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/google/code/gson',
    '3rdparty/jvm/org/apache/zookeeper:zookeeper-client',
    'science/src/java/com/twitter/common/io:io',
    'science/src/java/com/twitter/common/quantity:quantity',
    'science/src/java/com/twitter/common/zookeeper:client',
    'science/src/java/com/twitter/common/zookeeper:server-set',
    'finagle/finagle-core',
    'finagle/finagle-serversets',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-cacheresolver/src/main/resources'
  ]
)

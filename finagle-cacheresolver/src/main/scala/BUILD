scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-cacheresolver',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty:gson',
    '3rdparty/jvm/org/apache/zookeeper',
    '3rdparty/jvm/com/twitter/common:io',
    '3rdparty/jvm/com/twitter/common:quantity',
    '3rdparty/jvm/com/twitter/common/zookeeper:client',
    '3rdparty/jvm/com/twitter/common/zookeeper:server-set',
    'finagle/finagle-core',
    'finagle/finagle-serversets',
    'util/util-core'
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-cacheresolver/src/main/resources'
  ]
)

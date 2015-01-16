java_library(name='java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-cacheresolver-java',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty:scala-library',
    '3rdparty/jvm/com/twitter/common/zookeeper:client',
    'finagle/finagle-core',
    'finagle/finagle-cacheresolver/src/main/scala',
    'util/util-core',
  ],
  sources=rglobs('*.java')
)

java_library(name='java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-memcached-java',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/google/guava',
    '3rdparty/jvm/com/twitter/common/zookeeper:client',
    '3rdparty:netty',
    '3rdparty:scala-library',
    'finagle/finagle-cacheresolver',
    'finagle/finagle-core',
    'finagle/finagle-memcached/src/main/scala',
    'util/util-core',
  ],
  sources=rglobs('*.java')
)

java_library(name='java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-memcachedx-java',
    repo = artifactory
  ),
  dependencies=[
    '3rdparty:guava',
    '3rdparty:netty',
    '3rdparty:scala-library',
    '3rdparty/jvm/com/twitter/common/zookeeper:client',
    'finagle/finagle-core',
    'finagle/finagle-memcachedx/src/main/scala',
    'util/util-core',
  ],
  sources=rglobs('*.java')
)

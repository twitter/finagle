java_library(
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-memcached-java',
    repo = artifactory
  ),
  dependencies=[
    '3rdparty/jvm/com/google/guava',
    'science/src/java/com/twitter/common/zookeeper:client',
    '3rdparty/jvm/org/scala-lang:scala-library',
    'finagle/finagle-memcached/src/main/scala',
    '3rdparty/jvm/com/google/inject:guice',
    'finagle/finagle-core/src/main/scala:scala',
    'util/util-core/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  sources=rglobs('*.java')
)

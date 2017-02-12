java_library(
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-memcached-java',
    repo = artifactory
  ),
  dependencies=[
    '3rdparty/jvm/com/google/guava',
    '3rdparty/jvm/com/google/inject:guice',
    '3rdparty/jvm/org/scala-lang:scala-library',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-memcached/src/main/scala',
    'src/java/com/twitter/common/zookeeper:client',
    'util/util-core/src/main/scala:scala',
    'util/util-function/src/main/java:java',
    'util/util-stats/src/main/scala:scala',
  ],
  sources=rglobs('*.java')
)

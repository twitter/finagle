java_library(
  provides = artifact(
    org = 'com.twitter.finagle.common',
    name = 'finagle-serversets-java',
    repo = artifactory,
  ),
  dependencies = [
    '3rdparty/jvm/com/google/code/findbugs:jsr305',
    '3rdparty/jvm/com/google/code/gson:gson',
    '3rdparty/jvm/com/google/guava',
    '3rdparty/jvm/org/apache/thrift:libthrift-0.5.0',
    '3rdparty/jvm/org/apache/zookeeper:zookeeper-client',
    'finagle/finagle-serversets/src/main/thrift:thrift-java',
    'util/util-core',
  ],
  sources=rglobs('*.java'),
  exports=[
    '3rdparty/jvm/com/google/code/findbugs:jsr305',
    '3rdparty/jvm/com/google/guava:guava',
    'finagle/finagle-serversets/src/main/thrift:thrift-java',
  ]

)

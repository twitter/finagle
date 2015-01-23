scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-swift',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/google/inject/extensions:guice-multibindings',
    '3rdparty/jvm/com/google/inject:guice',
    '3rdparty/jvm/org/slf4j:slf4j-api',
    '3rdparty/jvm/com/twitter/com/facebook/swift:swift-codec',
    '3rdparty/jvm/org/apache/thrift:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-thrift',
  ],
  sources=rglobs('*.scala'),
  java_sources=['finagle/finagle-swift/src/main/java'],
)

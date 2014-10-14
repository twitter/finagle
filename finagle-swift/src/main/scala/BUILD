scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-swift',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty:swift-codec',
    '3rdparty:guice',
    '3rdparty:guice-multibindings',
    '3rdparty:slf4j-api',
    '3rdparty:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-thrift'
  ],
  sources=rglobs('*.scala'),
  java_sources=['finagle/finagle-swift/src/main/java'],
)

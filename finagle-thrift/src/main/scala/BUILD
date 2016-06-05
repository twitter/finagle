scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-thrift',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    '3rdparty/jvm/org/apache/thrift:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-thrift/src/main/java',
    'scrooge/scrooge-core',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
  resources=[
    'finagle/finagle-thrift/src/main/resources'
  ],
)

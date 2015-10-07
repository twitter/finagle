junit_tests(name='scala',
  dependencies=[
    '3rdparty/jvm/junit',
    '3rdparty/jvm/org/mockito:mockito-all',
    '3rdparty/jvm/io/netty',
    '3rdparty/jvm/org/scalatest',
    '3rdparty/jvm/org/scala-tools/testing:specs',
    '3rdparty/jvm/org/apache/thrift:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-test',
    'finagle/finagle-thrift',
    'finagle/finagle-thrift/src/test/java',
    'finagle/finagle-thrift/src/test/thrift',
    'util/util-core',

    ':silly-thrift-dep',
  ],
  resources=[
    'finagle/finagle-thrift/src/test/resources',
  ],
  sources=rglobs('*.scala')
)

jar_library(name='silly-thrift-dep',
  jars=[
    jar(org='silly', name='silly-thrift', rev='0.5.0'),
  ],
)

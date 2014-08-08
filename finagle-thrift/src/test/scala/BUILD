junit_tests(name='scala',
  dependencies=[
    '3rdparty:junit',
    '3rdparty:mockito-all',
    '3rdparty:netty',
    '3rdparty:scalatest',
    '3rdparty:specs',
    '3rdparty:thrift-0.5.0',
    'finagle/finagle-core',
    'finagle/finagle-test',
    'finagle/finagle-thrift',
    'finagle/finagle-thrift/src/test/java',
    'util/util-core',

    ':silly-thrift-dep',
  ],
  sources=rglobs('*.scala')
)

jar_library(name='silly-thrift-dep',
  jars=[
    jar(org='silly', name='silly-thrift', rev='0.5.0'),
  ],
)

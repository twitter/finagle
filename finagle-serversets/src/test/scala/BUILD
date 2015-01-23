target(name='scala',
  dependencies=[
    ':client',
    ':defaultloader',
    ':serversets'
  ]
)

target(name='core',
  dependencies=[
    '3rdparty/jvm/com/twitter/common/zookeeper:client',
    '3rdparty/jvm/com/twitter/common/zookeeper:server-set',
    '3rdparty/jvm/com/twitter/common:io',
    '3rdparty/jvm/com/twitter/common:quantity',
    '3rdparty/jvm/org/apache/zookeeper',
    '3rdparty/jvm/junit',
    '3rdparty/jvm/org/mockito:mockito-all',
    '3rdparty/jvm/io/netty',
    '3rdparty/jvm/org/scalatest',
    '3rdparty/jvm/org/scala-tools/testing:specs',
    'finagle/finagle-core',
    'finagle/finagle-serversets',
    'finagle/finagle-test',
    'util/util-core'
  ],
)

junit_tests(name='defaultloader',
  dependencies=[':core'],
  sources=globs('com/twitter/finagle/serverset2/client/apache/*.scala')
)

junit_tests(name='client',
  dependencies=[':core'],
  sources=globs('com/twitter/finagle/serverset2/client/*.scala')
)

junit_tests(name='serversets',
  dependencies=[':core'],
  resources=[
    'finagle/finagle-serversets/src/test/resources'
  ],
  sources=globs(
    'com/twitter/finagle/*.scala',
    'com/twitter/finagle/serverset2/*.scala',
    'com/twitter/finagle/zookeeper/*.scala',
  )
)

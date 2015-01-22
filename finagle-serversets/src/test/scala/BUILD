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
    '3rdparty:junit',
    '3rdparty:mockito-all',
    '3rdparty:netty',
    '3rdparty:scalatest',
    '3rdparty:specs',
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

target(name='scala',
  dependencies=[
    ':client',
    ':defaultloader',
    ':serversets'
  ]
)

target(name='core',
  dependencies=[
    'science/src/java/com/twitter/common/zookeeper:client',
    'science/src/java/com/twitter/common/zookeeper:server-set',
    'science/src/java/com/twitter/common/io:io',
    'science/src/java/com/twitter/common/quantity:quantity',
    '3rdparty/jvm/org/apache/zookeeper:zookeeper-server',
    '3rdparty/jvm/junit',
    '3rdparty/jvm/org/mockito:mockito-all',
    '3rdparty/jvm/org/scalatest',
    'finagle/finagle-core',
    'finagle/finagle-serversets',
    'util/util-core',
    'util/util-zk-test'
  ],
)

junit_tests(name='defaultloader',
  dependencies=[':core'],
  sources=globs('com/twitter/finagle/serverset2/client/apache/*.scala')
)

junit_tests(name='client',
  dependencies=[':core'],
  fatal_warnings=True,
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
    'com/twitter/finagle/serverset2/addr/*.scala',
    'com/twitter/finagle/zookeeper/*.scala',
  )
)

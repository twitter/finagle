junit_tests(name='scala',
  dependencies=[
    pants(':client'),
    pants(':defaultloader'),
    pants(':serversets')
  ]
)

junit_tests(name='core',
  dependencies=[
    pants('3rdparty:junit'),
    pants('3rdparty:mockito-all'),
    pants('3rdparty:netty'),
    pants('3rdparty:scalatest'),
    pants('3rdparty:specs'),
    pants('3rdparty:zookeeper'),
    pants('3rdparty/jvm/com/twitter/common:io'),
    pants('3rdparty/jvm/com/twitter/common:quantity'),
    pants('3rdparty/jvm/com/twitter/common/zookeeper:client'),
    pants('3rdparty/jvm/com/twitter/common/zookeeper:server-set'),
    pants('finagle/finagle-core'),
    pants('finagle/finagle-test'),
    pants('finagle/finagle-serversets'),
    pants('util/util-core')
  ]
)

junit_tests(name='defaultloader',
  dependencies=[pants(':core')],
  sources=globs('com/twitter/finagle/serverset2/client/apache/*.scala')
)

junit_tests(name='client',
  dependencies=[pants(':core')],
  sources=globs('com/twitter/finagle/serverset2/client/*.scala')
)

junit_tests(name='serversets',
  dependencies=[pants(':core')],
  sources=globs(
    'com/twitter/finagle/serverset2/*.scala',
    'com/twitter/finagle/zookeeper/*.scala',
  )
)

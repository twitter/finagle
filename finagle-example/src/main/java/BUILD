java_library(name='java',
  provides = artifact(
    org = 'com.twitter',
    name = 'finagle-example-java',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/io/netty',
    'finagle/finagle-example/src/main/thrift:thrift-scala',
    'finagle/finagle-kestrel',
    'finagle/finagle-thrift',
    'util/util-core',
    '3rdparty/jvm/com/fasterxml/jackson/core:jackson-databind',
    '3rdparty/jvm/com/fasterxml/jackson/module:jackson-module-scala',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-http/src/main/scala:scala',
    'scrooge/scrooge-core/src/main/scala:scala',
  ],
  fatal_warnings=False,
  sources=rglobs('*.java')
)

jvm_binary(name='thrift-server',
  main='com.twitter.finagle.example.java.thrift.ThriftServer',
  dependencies=[
    ':java'
  ]
)

jvm_binary(name='thrift-client',
  main='com.twitter.finagle.example.java.thrift.ThriftClient',
  dependencies=[
    ':java'
  ]
)

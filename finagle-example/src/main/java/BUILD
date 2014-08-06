java_library(name='java',
  dependencies=[
    '3rdparty:netty',
    'finagle/finagle-core',
    'finagle/finagle-example/src/main/thrift:thrift-scala',
    'finagle/finagle-kestrel',
    'finagle/finagle-thrift',
    'util/util-core',
  ],
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

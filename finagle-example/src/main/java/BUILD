java_library(name='java',
  dependencies=[
    pants('3rdparty:netty'),
    pants('finagle/finagle-core'),
    pants('finagle/finagle-example/src/main/thrift:thrift-scala'),
    pants('finagle/finagle-kestrel'),
    pants('finagle/finagle-thrift'),
    pants('util/util-core'),
  ],
  sources=rglobs('*.java')
)

jvm_binary(name='thrift-server',
  main='com.twitter.finagle.example.java.thrift.ThriftServer',
  dependencies=[
    pants(':java')
  ]
)

jvm_binary(name='thrift-client',
  main='com.twitter.finagle.example.java.thrift.ThriftClient',
  dependencies=[
    pants(':java')
  ]
)

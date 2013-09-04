scala_library(name='scala',
  dependencies=[
    pants('3rdparty:caliper'),
    pants('3rdparty:netty'),
    pants('3rdparty:scala-library'),
    pants('finagle/finagle-core'),
    pants('finagle/finagle-http'),
    pants('finagle/finagle-ostrich4'),
    pants('finagle/finagle-thrift'),
    pants('finagle/finagle-thriftmux'),
    pants('ostrich'),
    pants('util/util-app'),
    pants('util/util-logging'),
    compiled_idl(
      idl_deps=pants('finagle/finagle-stress/src/main/thrift'),
      compiler='scrooge-legacy',
      language='scala',
      rpc_style='finagle'
    )
  ],
  sources=rglobs('*.scala')
)

benchmark(name='bench',
  dependencies=[
    pants(":scala")
  ]
)

jvm_binary(name='stress',
  main='com.twitter.finagle.stress.LoadBalancerTest',
  dependencies=[
    pants(':scala')
  ]
)



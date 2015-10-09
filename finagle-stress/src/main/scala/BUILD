scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-stress',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/google/caliper',
    '3rdparty/jvm/io/netty',
    '3rdparty/jvm/org/scala-lang:scala-library',
    'finagle/finagle-core',
    'finagle/finagle-httpx',
    'finagle/finagle-ostrich4',
    'finagle/finagle-stress/src/main/thrift:thrift-scala',
    'finagle/finagle-thrift',
    'finagle/finagle-thriftmux',
    'ostrich',
    'util/util-app',
    'util/util-logging',
  ],
  sources=rglobs('*.scala'),
)

benchmark(name='bench',
  dependencies=[
    ":scala"
  ]
)

jvm_binary(name='stress',
  main='com.twitter.finagle.stress.LoadBalancerTest',
  dependencies=[
    ':scala'
  ]
)



scala_library(name='scala',
  dependencies=[
    '3rdparty/jvm/org/apache/curator:curator-framework',
    '3rdparty/jvm/org/apache/curator:curator-test',
    '3rdparty/jvm/org/openjdk/jmh:jmh-core',
    '3rdparty/jvm/org/scala-lang:scala-library',
    'finagle/finagle-benchmark-thrift/src/main/thrift:thrift-scala',
    'finagle/finagle-commons-stats',
    'finagle/finagle-core',
    'finagle/finagle-exp',
    'finagle/finagle-memcached',
    'finagle/finagle-mux',
    'finagle/finagle-ostrich4',
    'finagle/finagle-serversets',
    'finagle/finagle-stats',
    'finagle/finagle-thriftmux',
    'finagle/finagle-zipkin',
    'util/util-app',
  ],
  sources=rglobs('*.scala')
)

# depend on this for resources/real_latencies.data
jar_library(name='metrics-samples',
  jars = [
    jar(org='com.twitter.common', name='metrics-data-sample', rev='0.0.1'),
  ],
)

jvm_binary(
  name='p2csim',
  main='com.twitter.finagle.loadbalancer.Simulation',
  dependencies=[':scala', ':metrics-samples']
)

jvm_binary(
  name='hello-server',
  main='com.twitter.finagle.thrift.HelloServer',
  dependencies=[':scala']
)

jvm_binary(
  name='serverset-service',
  main='com.twitter.finagle.serverset2.LocalServerSetService',
  dependencies=[':scala']
)

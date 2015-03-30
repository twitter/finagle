scala_library(name='scala',
  dependencies=[
    '3rdparty/jvm/com/google/caliper',
    '3rdparty/jvm/org/openjdk/jmh:jmh-core',
    '3rdparty/jvm/org/scala-lang:scala-library',
    'finagle/finagle-core',
    'finagle/finagle-memcached',
    'finagle/finagle-ostrich4',
    'finagle/finagle-stats',
    'finagle/finagle-zipkin',
    'finagle/finagle-swift',
    'finagle/finagle-benchmark/src/main/thrift:thrift-scala',
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

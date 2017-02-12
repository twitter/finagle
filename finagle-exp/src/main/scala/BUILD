scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-exp',
    repo = artifactory,
  ),
  dependencies=[
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-thrift/src/main/java:java',
    'util/util-core/src/main/scala:scala',
    'util/util-logging/src/main/scala:scala',
    'util/util-stats/src/main/scala:scala',
  ],
  fatal_warnings=True,
  sources=rglobs('com/twitter/finagle/exp/*.scala'),
)

benchmark(name='benchmark',
  dependencies=[
    ':scala',
    'util/util-core',
  ],
  sources=rglobs('com/twitter/finagle/benchmark/*.scala')
)

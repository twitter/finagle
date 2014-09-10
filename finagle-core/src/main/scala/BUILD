scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-core',
    repo = 'pants-support/ivy:internal',
  ),
  dependencies=[
    '3rdparty:guava',
    '3rdparty:netty',
    'util/util-app',
    'util/util-collection',
    'util/util-core',
    'util/util-hashing',
    'util/util-jvm',
    'util/util-logging',
    'finagle/finagle-core/src/main/java',
  ],
  sources=rglobs('*.scala'),
)

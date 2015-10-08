scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-http',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/google/guava',
    '3rdparty/jvm/com/twitter/bijection:core',
    '3rdparty/jvm/commons-lang',
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'util/util-codec',
    'util/util-core',
    'util/util-logging',
  ],
  sources=rglobs('*.scala'),
)

scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle.httpx',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty:commons-lang',
    '3rdparty:guava',
    '3rdparty:netty',
    '3rdparty/jvm/com/twitter/bijection:core',
    'finagle/finagle-core',
    'util/util-logging',
    'util/util-codec',
    'util/util-core',
  ],
  sources=rglobs('*.scala'),
)

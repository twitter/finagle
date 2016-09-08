scala_library(name='scala',
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-http',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/google/code/findbugs:jsr305',
    '3rdparty/jvm/com/google/guava',
    '3rdparty/jvm/commons-lang',
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core/src/main/scala:scala',
    'finagle/finagle-toggle/src/main/scala:scala',
    scoped('util/util-app/src/main/scala', scope='forced'),
    'util/util-codec',
    'util/util-collection',
    'util/util-core/src/main/scala:scala',
    'util/util-logging',
    'util/util-stats',
  ],
  resources=[
    'finagle/finagle-http/src/main/resources'
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)

scala_library(
  provides = scala_artifact(
    org = 'com.twitter',
    name = 'finagle-base-http',
    repo = artifactory,
  ),
  dependencies=[
    '3rdparty/jvm/com/google/code/findbugs:jsr305',
    '3rdparty/jvm/commons-lang',
    '3rdparty/jvm/io/netty',
    'finagle/finagle-core',
    'util/util-collection'
  ],
  fatal_warnings=True,
  sources=rglobs('*.scala'),
)
